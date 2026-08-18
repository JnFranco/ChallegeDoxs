import json
import logging
from decimal import Decimal

from confluent_kafka import Consumer, KafkaError

from app.config import settings
from app.fraud import evaluate_fraud
from app.producer import publish_fraud_decision

logger = logging.getLogger(__name__)

consumer = Consumer({
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "anti-fraud-service",
    "auto.offset.reset": "earliest",
})


def process_transaction(message):
    """
    Procesa una transacción recibida desde Kafka,
    evalúa la regla de fraude y publica la decisión.
    """
    data = json.loads(message.value().decode("utf-8"))

    transaction_external_id = data["transactionExternalId"]
    value = Decimal(str(data["value"]))

    status = evaluate_fraud(value)
    publish_fraud_decision(transaction_external_id, status)


def start_consumer():
    consumer.subscribe(["transactions.created"])
    logger.info("Anti-Fraud Service escuchando transactions.created")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Error de Kafka: %s", msg.error())
                continue

            try:
                process_transaction(msg)
            except Exception:
                logger.exception("Error al procesar mensaje")
    except KeyboardInterrupt:
        logger.info("Deteniendo Anti-Fraud Service")
    finally:
        consumer.close()
