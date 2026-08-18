import json
import logging
import threading
from uuid import UUID

from confluent_kafka import Consumer, KafkaError

from app.config import settings
from app.database import SessionLocal
from app.models import Transaction

logger = logging.getLogger(__name__)

consumer = Consumer({
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "transaction-service-fraud-decisions",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

FINAL_STATUSES = {"approved", "rejected"}

shutdown_event = threading.Event()
consumer_thread = None


def process_decision(message):
    """
    Procesa una decisión de fraude recibida desde Kafka.

    La actualización de la transacción se confirma en PostgreSQL
    antes de confirmar el offset del mensaje para evitar perder
    una decisión si ocurre un error durante la persistencia.
    """
    data = json.loads(message.value().decode("utf-8"))

    tx_id = data.get("transactionExternalId")
    status = data.get("status")

    if not tx_id or not status:
        raise ValueError("Faltan campos obligatorios")

    try:
        UUID(tx_id)
    except ValueError:
        raise ValueError(f"transactionExternalId inválido: {tx_id}")

    if status not in FINAL_STATUSES:
        raise ValueError(f"status inválido: {status}")

    db = SessionLocal()
    try:
        transaction = db.query(Transaction).filter(
            Transaction.transaction_external_id == tx_id
        ).first()

        if not transaction:
            logger.warning("Transacción %s no encontrada, se omite", tx_id)
            return True

        # Si la transacción ya tiene un estado final, no se modifica.
        # Esto garantiza idempotencia ante mensajes duplicados.
        if transaction.status in FINAL_STATUSES:
            if transaction.status == status:
                logger.info(
                    "Transacción %s ya está en %s, se omite duplicado",
                    tx_id, status,
                )
            else:
                logger.warning(
                    "Transacción %s ya está en %s, se ignora decisión %s",
                    tx_id, transaction.status, status,
                )
            return True

        if status == transaction.status:
            return True

        transaction.status = status
        db.commit()
        logger.info("Transacción %s actualizada a %s", tx_id, status)
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def start_consumer():
    consumer.subscribe(["fraud.decisions"])
    logger.info("Transaction Service escuchando fraud.decisions")

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Error de Kafka: %s", msg.error())
                continue

            try:
                result = process_decision(msg)
                if result:
                    consumer.commit(asynchronous=False)
            except ValueError as e:
                # Mensajes inválidos: se confirma el offset para no bloquear el consumer.
                # En un entorno real se usaría una Dead Letter Queue.
                logger.warning("Mensaje inválido: %s", e)
                consumer.commit(asynchronous=False)
            except Exception:
                logger.exception("Error al procesar mensaje")
    except Exception:
        logger.exception("Error inesperado en el loop del consumer")
    finally:
        consumer.close()
        logger.info("Consumer de Kafka cerrado")


def start_consumer_thread():
    global consumer_thread
    shutdown_event.clear()
    consumer_thread = threading.Thread(target=start_consumer)
    consumer_thread.start()
    logger.info("Hilo del consumer de Kafka iniciado")


def stop_consumer():
    logger.info("Deteniendo consumer de Kafka...")
    shutdown_event.set()
    if consumer_thread:
        consumer_thread.join(timeout=10)
    logger.info("Consumer de Kafka detenido")
