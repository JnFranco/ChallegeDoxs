# Sistema de Procesamiento de Transacciones

Sistema de procesamiento de transacciones compuesto por dos servicios independientes que se comunican mediante Kafka. Un cliente crea una transacción, esta se guarda como `pending`, un servicio de antifraude evalúa una regla de negocio y la transacción termina como `approved` o `rejected`.

## Arquitectura

```
┌──────────┐     ┌─────────────────────┐     ┌────────────┐
│          │────▶│ Transaction Service  │────▶│ PostgreSQL │
│  Cliente │     │  (FastAPI)           │     │            │
│          │◀────│  POST /transactions  │◀────│            │
└──────────┘     └─────────┬───────────┘     └────────────┘
                           │
                     Outbox Publisher
                           │
                           ▼
                    ┌──────────────┐
                    │    Kafka     │
                    │              │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ Anti-Fraud   │
                    │ Service      │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │    Kafka     │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ Transaction  │
                    │ Consumer     │
                    └──────────────┘
```

## Flujo de una transacción

1. El cliente envía `POST /transactions` con los datos de la transferencia.
2. Se validan los campos (UUIDs, cuentas distintas, transferTypeId válido, value > 0).
3. Se crea una `Transaction` con estado `pending` y un `OutboxEvent` pendiente de publicación.
4. Ambos se confirman en PostgreSQL con un solo `COMMIT`.
5. El Outbox Publisher (un hilo en segundo plano) detecta el evento pendiente.
6. Publica `transactions.created` en Kafka.
7. Anti-Fraud Service consume el evento y evalúa la regla de fraude.
8. Publica `fraud.decisions` en Kafka con el resultado (`approved` o `rejected`).
9. El consumer de Transaction Service consume la decisión y actualiza la transacción.
10. La transacción queda en su estado final (`approved` o `rejected`).

## API

### Crear Transacción

```bash
curl -X POST http://localhost:8000/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "accountExternalIdDebit": "2b894fb0-09f1-4d46-a610-0c92a5c4e113",
    "accountExternalIdCredit": "045d5400-e3cf-4e57-9fe4-a9815eeec2c4",
    "transferTypeId": 1,
    "value": 120
  }'
```

Respuesta `201 Created`:
```json
{
  "transactionExternalId": "d6674f5f-c4bc-4e74-9236-66370946c625"
}
```

Validaciones:
- `accountExternalIdDebit` y `accountExternalIdCredit` deben ser UUIDs distintos.
- `transferTypeId` debe existir en la tabla `transfer_types`.
- `value` debe ser mayor que 0.

### Consultar Transacción

```bash
curl http://localhost:8000/transactions/{transactionExternalId}
```

Respuesta `200 OK`:
```json
{
  "transactionExternalId": "d6674f5f-c4bc-4e74-9236-66370946c625",
  "transactionType": { "name": "transfer" },
  "transactionStatus": { "name": "pending" },
  "value": 120,
  "createdAt": "2026-01-15T13:45:30.000Z"
}
```

Si no existe, retorna `404` con `"Transacción no encontrada"`.

### Health Check

```bash
curl http://localhost:8000/health
```

Respuesta: `{"status": "ok"}`

### Swagger UI

Disponible en http://localhost:8000/docs

## Regla Antifraude

- `value <= 1000` → `approved`
- `value > 1000` → `rejected`

Ejemplos:
| value | resultado |
|-------|-----------|
| 120 | approved |
| 1000 | approved |
| 1000.01 | rejected |
| 1001 | rejected |

## Persistencia

### transfer_types

Tipos de transferencia disponibles. Se crea un registro semilla: `id=1, name=transfer`.

### transactions

Almacena cada transacción creada. Estados posibles: `pending`, `approved`, `rejected`.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | BIGINT PK | ID interno |
| transaction_external_id | UUID | ID público para el cliente |
| account_external_id_debit | UUID | Cuenta de origen |
| account_external_id_credit | UUID | Cuenta de destino |
| transfer_type_id | INT FK | Referencia a transfer_types |
| value | NUMERIC(12,2) | Monto de la transacción |
| status | VARCHAR(20) | Estado actual |
| created_at | TIMESTAMPTZ | Fecha de creación |
| updated_at | TIMESTAMPTZ | Última actualización |

### outbox_events

Eventos pendientes de publicación en Kafka. Implementa el patrón Outbox.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | BIGINT PK | ID interno |
| event_id | UUID | ID único del evento |
| event_type | VARCHAR(50) | Nombre del topic (ej: `transactions.created`) |
| aggregate_id | UUID | ID de la transacción relacionada |
| payload | TEXT | JSON serializado con los datos |
| status | VARCHAR(20) | `pending`, `published` o `dlq` |
| created_at | TIMESTAMPTZ | Fecha de creación |
| published_at | TIMESTAMPTZ | Fecha de publicación (nullable) |
| attempts | INT | Intentos de publicación fallidos |
| next_retry_at | TIMESTAMPTZ | Próximo reintento programado (nullable) |

## Outbox Pattern

El problema original: después de hacer `COMMIT` en PostgreSQL, si Kafka no está disponible, la transacción queda como `pending` para siempre y nunca se evalúa.

La solución: en vez de publicar a Kafka directamente desde el endpoint, se guarda un `OutboxEvent` en la misma transacción que la `Transaction`. Un publisher en segundo plano publica los eventos pendientes.

```
POST /transactions
    ↓
Transaction (pending) + OutboxEvent (pending)
    ↓
UN SOLO COMMIT en PostgreSQL
    ↓
Outbox Publisher detecta el evento
    ↓
Publica a Kafka
    ↓
Marca como published
```

Esto garantiza que no se pierden eventos incluso si Kafka cae temporalmente.

## Retries

El Outbox Publisher reintenta publicar eventos que fallan:

| Intento | Backoff antes del siguiente |
|---------|----------------------------|
| 1 | 2 segundos |
| 2 | 4 segundos |
| 3 | Marca como `dlq` |

`MAX_ATTEMPTS = 3`. El backoff es exponencial: `2^attempts` segundos.

Cada evento tiene su propio `next_retry_at`, así un evento en backoff no bloquea otros eventos pendientes.

## DLQ (Dead Letter Queue)

Cuando un evento falla 3 veces consecutivas, se marca como `dlq` en PostgreSQL. El evento permanece almacenado con su payload intacto para revisión o reprocesamiento manual.

No se publica a un topic de Kafka adicional. La DLQ es simplemente un estado en la tabla `outbox_events`.

## Idempotencia

El sistema usa entrega **at-least-once**. Pueden existir mensajes duplicados si:

1. El publisher envía el mensaje a Kafka exitosamente.
2. El proceso muere antes de actualizar el estado en PostgreSQL.
3. Al reiniciar, el evento sigue como `pending` y se reenvía.

El consumer de `fraud.decisions` es idempotente: si la transacción ya tiene un estado final (`approved` o `rejected`), ignora el mensaje duplicado.

## Concurrencia

El Outbox Publisher usa `SELECT ... FOR UPDATE SKIP LOCKED` de PostgreSQL para evitar que dos instancias del publisher procesen el mismo evento simultáneamente. En la práctica se ejecuta una sola instancia del publisher por servicio.

## Tests

### Tests unitarios (Transaction Service)

```bash
cd transaction-service
pip install -r requirements.txt
# Requiere PostgreSQL corriendo en localhost:5432
TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/transactions_db_test" \
  pytest tests/ -v
```

Tests incluidos:
- **API** (17 tests): validaciones, creación, consulta, atomicidad
- **Kafka Consumer** (10 tests): transiciones de estado, idempotencia, validación de mensajes
- **Outbox Model** (11 tests): tabla, columnas, defaults, queries
- **Outbox Atomicity** (10 tests): creación conjunta, payload, sin publicación directa
- **Outbox Publisher** (15 tests): publicación, retries, backoff, DLQ, shutdown

### Tests unitarios (Anti-Fraud Service)

```bash
cd anti-fraud-service
pip install -r requirements.txt
pytest tests/ -v
```

### Tests de integración

Requiere todos los servicios corriendo via Docker Compose.

```bash
pip install httpx pytest
pytest tests/test_integration.py -v
```

Verifica el flujo completo: POST → pending → outbox → Kafka → Anti-Fraud → fraud.decisions → approved/rejected.

## Ejecución

### Levantar el sistema

```bash
docker compose up --build
```

Servicios:
| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| Transaction Service | 8000 | REST API + Outbox Publisher + Kafka consumer |
| Anti-Fraud Service | — | Kafka consumer/producer |
| PostgreSQL | 5432 | Base de datos |
| Kafka | 9092 (host), 29092 (interno) | Message broker |

Señales de que todo está listo:
- `Topics created successfully` (kafka-init)
- `Hilo del consumer de Kafka iniciado`
- `Hilo del Outbox Publisher iniciado`
- `Anti-Fraud Service escuchando transactions.created`

### Detener el sistema

```bash
docker compose down
```

### Eliminar todos los datos

```bash
docker compose down -v
```

## Estructura del Proyecto

```
├── transaction-service/
│   ├── app/
│   │   ├── main.py              # Endpoints FastAPI + startup/shutdown
│   │   ├── models.py            # Modelos SQLAlchemy (Transaction, OutboxEvent)
│   │   ├── schemas.py           # Schemas Pydantic (validación)
│   │   ├── database.py          # Conexión a PostgreSQL
│   │   ├── config.py            # Variables de entorno
│   │   ├── db_init.py           # Creación de tablas + seed
│   │   ├── kafka_producer.py    # Publica eventos a Kafka
│   │   ├── kafka_consumer.py    # Consume fraud.decisions
│   │   └── outbox_publisher.py  # Publica eventos pendientes del outbox
│   ├── tests/
│   ├── requirements.txt
│   └── Dockerfile
├── anti-fraud-service/
│   ├── app/
│   │   ├── main.py              # Punto de entrada
│   │   ├── fraud.py             # Regla antifraude
│   │   ├── consumer.py          # Consume transactions.created
│   │   ├── producer.py          # Publica fraud.decisions
│   │   └── config.py            # Variables de entorno
│   ├── tests/
│   ├── requirements.txt
│   └── Dockerfile
├── tests/
│   └── test_integration.py      # Tests de flujo completo
├── docker-compose.yml
└── README.md
```

## Decisiones y Trade-offs

**¿Por qué Python/FastAPI?** Es un lenguaje directo con un ecosistema sólido para servicios web. FastAPI genera documentación automática y maneja validación con Pydantic.

**¿Por qué PostgreSQL?** Es robusto, confiable y soporta `FOR UPDATE SKIP LOCKED` nativo, que se usa para la concurrencia del Outbox Publisher.

**¿Por qué Kafka?** Es el mecanismo de comunicación entre servicios que pide el challenge. Soporta retención de mensajes, múltiples consumidores y tolerancia a fallos.

**¿Por qué Outbox?** Elimina la ventana de inconsistencia donde una transacción se guarda pero el evento no se publica. Es la solución estándar para este problema.

**¿Por qué at-least-once?** Es el nivel de entrega más práctico para este caso. Los consumidores son idempotentes, así que los duplicados no causan efectos secundarios.

**¿Por qué consumidores idempotentes?** Porque en un sistema distribuido los mensajes pueden duplicarse. La idempotencia garantiza que procesar el mismo mensaje dos veces tiene el mismo efecto que procesarlo una vez.

**¿Por qué no GraphQL?** El challenge pide una REST API. GraphQL agregaría complejidad innecesaria sin un caso de uso claro que lo justifique.

### Tareas externas opcionales completadas

De las tareas opcionales del challenge, este proyecto implementa 2:

1. **Fortalecer la entrega de eventos**: Outbox transaccional, consumidores idempotentes, reintentos con backoff exponencial y dead-letter queue (estado `dlq` en PostgreSQL).
2. **Señales operacionales**: Health check endpoint (`/health`) y logs estructurados con `logging` en todos los componentes críticos (consumer, publisher, producer).

### Limitaciones conocidas

- El Outbox Publisher corre como un hilo dentro de transaction-service. En un entorno real se deployaría como un servicio separado.
- No hay métricas ni tracing. Sería lo siguiente a agregar en un entorno real.
- Los tests unitarios necesitan PostgreSQL corriendo. No usan SQLite por compatibilidad con features de PostgreSQL (`FOR UPDATE SKIP LOCKED`).
