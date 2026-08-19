import json
import logging
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from sqlalchemy.orm import Session

from app.database import get_db
from app.db_init import init_db
from app.kafka_consumer import start_consumer_thread, stop_consumer
from app.outbox_publisher import start_outbox_publisher, stop_outbox_publisher
from app.models import OutboxEvent, Transaction, TransferType
from app.schemas import (
    TransactionCreatedResponse,
    TransactionCreate,
    TransactionResponse,
    TransactionStatusOut,
    TransactionTypeOut,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Transaction Service API",
    description="""
## Servicio de Procesamiento de Transacciones

Este servicio permite crear transacciones financieras y consultar su estado.

### Flujo
1. **Crear transacción** → se guarda como `pending` en la base de datos
2. **Anti-Fraud evalúa** → el servicio de antifraude analiza la transacción
3. **Resultado** → la transacción cambia a `approved` (aprobada) o `rejected` (rechazada)

### Regla Antifraude
- Monto **<= 1000** → aprobada (`approved`)
- Monto **> 1000** → rechazada (`rejected`)

### Cómo usar esta API
1. De click en **POST /transactions** y luego en **Try it out**
2. Complete los campos (use los UUIDs de ejemplo que aparecen)
3. De click en **Execute** — recibirá un `transactionExternalId`
4. Copie ese ID y colóquelo en **GET /transactions/{transaction_external_id}**
5. De click en **Execute** — verá la transacción con estado `pending`
6. Espere **5-10 segundos** y ejecute **GET** de nuevo — el estado cambió a `approved` o `rejected`

### Notas importantes
- La evaluación de fraude es **asíncrona**. Recién creada, la transacción tiene estado `pending`
- Después de unos segundos, el estado cambia a `approved` o `rejected`
- Si el monto es mayor a 1000, será rechazada

### Cómo leer el Swagger UI
- **Example Value** = es solo un ejemplo del formato de respuesta, NO es una transacción real
- **Response body** (arriba) = es la respuesta **real** de la API cuando se hace "Try it out"
- Solo importa el **Response body** que aparece después de hacer Execute
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None,
)


@app.on_event("startup")
def startup():
    init_db()
    start_consumer_thread()
    start_outbox_publisher()


@app.on_event("shutdown")
def shutdown():
    stop_outbox_publisher()
    stop_consumer()


@app.post(
    "/transactions",
    status_code=201,
    response_model=TransactionCreatedResponse,
    summary="Crear una transacción",
    description=(
        "Crea una nueva transacción con estado **pending**. "
        "Se valida que las cuentas sean distintas, que el tipo de transferencia exista "
        "y que el monto sea mayor que 0.\n\n"
        "**Qué hacer:** De click en \"Try it out\", complete los campos y de click en \"Execute\".\n\n"
        "**Respuesta:** El UUID de la transacción creada. Cópielo y úselo en GET para consultar el estado.\n\n"
        "**Después:** La transacción queda como `pending`. El servicio Anti-Fraud la evalúa "
        "asincrónicamente y la cambia a `approved` o `rejected`."
    ),
    tags=["Transacciones"],
    responses={
        201: {"description": "Transacción creada exitosamente. Devuelve el UUID de la transacción."},
        422: {"description": "Datos inválidos. Verifique que las cuentas sean distintas, que el tipo de transferencia sea 1 y que el monto sea mayor a 0."},
    },
)
def create_transaction(body: TransactionCreate, db: Session = Depends(get_db)):
    # Las cuentas débito y crédito deben ser distintas.
    if body.accountExternalIdDebit == body.accountExternalIdCredit:
        raise HTTPException(
            status_code=422,
            detail="Las cuentas de origen y destino deben ser distintas",
        )

    # El tipo de transferencia debe existir en la tabla transfer_types.
    transfer_type = db.get(TransferType, body.transferTypeId)
    if not transfer_type:
        raise HTTPException(
            status_code=422,
            detail=f"El tipo de transferencia {body.transferTypeId} no existe. Use 1 para transferencia estándar.",
        )

    tx_external_id = uuid4()

    transaction = Transaction(
        transaction_external_id=tx_external_id,
        account_external_id_debit=body.accountExternalIdDebit,
        account_external_id_credit=body.accountExternalIdCredit,
        transfer_type_id=body.transferTypeId,
        value=body.value,
    )

    payload = {
        "transactionExternalId": str(tx_external_id),
        "accountExternalIdDebit": str(body.accountExternalIdDebit),
        "accountExternalIdCredit": str(body.accountExternalIdCredit),
        "transferTypeId": body.transferTypeId,
        "value": float(body.value),
    }

    outbox_event = OutboxEvent(
        event_id=uuid4(),
        event_type="transactions.created",
        aggregate_id=tx_external_id,
        payload=json.dumps(payload),
    )

    try:
        db.add(transaction)
        db.add(outbox_event)
        db.commit()
        db.refresh(transaction)
    except Exception:
        db.rollback()
        raise

    return TransactionCreatedResponse(
        transactionExternalId=transaction.transaction_external_id,
    )


@app.get(
    "/transactions/{transaction_external_id}",
    response_model=TransactionResponse,
    summary="Consultar una transacción",
    description=(
        "Devuelve la transacción completa por su UUID.\n\n"
        "**Qué hacer:** Coloque el `transactionExternalId` que recibió al crear la transacción "
        "y de click en \"Execute\".\n\n"
        "**Respuesta:** Los datos de la transacción:\n"
        "- `transactionType.name` → tipo de transferencia\n"
        "- `transactionStatus.name` → estado actual: `pending`, `approved` o `rejected`\n"
        "- `value` → monto\n"
        "- `createdAt` → fecha de creación\n\n"
        "**Importante:** Si recién creó la transacción, el estado es `pending`. "
        "Espere 5-10 segundos y vuelva a ejecutar para ver el resultado."
    ),
    tags=["Transacciones"],
    responses={
        200: {"description": "Transacción encontrada. El estado puede ser pending, approved o rejected."},
        404: {"description": "No existe una transacción con ese UUID. Verifique que el ID sea correcto."},
    },
)
def get_transaction(transaction_external_id: UUID, db: Session = Depends(get_db)):
    transaction = (
        db.query(Transaction)
        .filter(Transaction.transaction_external_id == transaction_external_id)
        .first()
    )
    if not transaction:
        raise HTTPException(status_code=404, detail="Transacción no encontrada")

    return TransactionResponse(
        transactionExternalId=transaction.transaction_external_id,
        transactionType=TransactionTypeOut(name=transaction.transfer_type.name),
        transactionStatus=TransactionStatusOut(name=transaction.status),
        value=transaction.value,
        createdAt=transaction.created_at,
    )


@app.get(
    "/health",
    summary="Verificar estado del servicio",
    description="Endpoint de health check. Retorna `{\"status\": \"ok\"}` si el servicio está funcionando.",
    tags=["Sistema"],
)
def health():
    return {"status": "ok"}
