import json
import logging
from decimal import Decimal
from uuid import UUID

from confluent_kafka import Producer

from app.config import settings

logger = logging.getLogger(__name__)

producer = Producer({
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
})


def _json_serializer(obj):
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Tipo {type(obj)} no es serializable a JSON")


def publish_event_to_kafka(topic, key, payload):
    """Publica un evento genérico en Kafka."""
    producer.produce(
        topic=topic,
        key=key,
        value=payload if isinstance(payload, str) else json.dumps(payload, default=_json_serializer),
    )
    producer.flush(timeout=10)
