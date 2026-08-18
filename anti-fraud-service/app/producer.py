import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer

from app.config import settings

logger = logging.getLogger(__name__)

producer = Producer({
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
})


def publish_fraud_decision(transaction_external_id: str, status: str):
    """
    Publica una decisión de fraude (approved/rejected) en Kafka.
    """
    payload = {
        "transactionExternalId": transaction_external_id,
        "status": status,
        "occurredAt": datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer.produce(
            topic="fraud.decisions",
            key=transaction_external_id,
            value=json.dumps(payload),
        )
        producer.flush(timeout=10)
        logger.info("Publicado fraud.decision para %s: %s", transaction_external_id, status)
    except Exception:
        logger.exception("Error al publicar fraud.decision para %s", transaction_external_id)
        raise
