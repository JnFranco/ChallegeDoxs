import logging
import threading
from datetime import datetime, timedelta, timezone

from app.database import SessionLocal
from app.kafka_producer import publish_event_to_kafka
from app.models import OutboxEvent

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
POLL_INTERVAL = 5
BACKOFF_BASE = 2

_stop_event = threading.Event()
_publisher_thread = None


def _process_event(event, session):
    """Intenta publicar un evento. Retorna True si tuvo éxito."""
    try:
        publish_event_to_kafka(
            topic=event.event_type,
            key=str(event.aggregate_id),
            payload=event.payload,
        )
        event.status = "published"
        event.published_at = datetime.now(timezone.utc)
        session.commit()
        logger.info("Evento %s publicado en %s", event.event_id, event.event_type)
        return True
    except Exception:
        event.attempts += 1
        if event.attempts >= MAX_ATTEMPTS:
            event.status = "dlq"
            logger.warning(
                "Evento %s enviado a DLQ tras %d intentos fallidos",
                event.event_id, event.attempts,
            )
        else:
            backoff = BACKOFF_BASE ** event.attempts
            event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff)
            logger.warning(
                "Fallo al publicar evento %s, reintento %d/%d en %ds",
                event.event_id, event.attempts, MAX_ATTEMPTS, backoff,
            )
        session.commit()
        return False


def _run():
    """Loop principal del Outbox Publisher."""
    logger.info("Outbox Publisher iniciado")
    while not _stop_event.is_set():
        session = SessionLocal()
        try:
            now = datetime.now(timezone.utc)
            events = (
                session.query(OutboxEvent)
                .filter(
                    OutboxEvent.status == "pending",
                    OutboxEvent.attempts < MAX_ATTEMPTS,
                )
                .with_for_update(skip_locked=True)
                .order_by(OutboxEvent.created_at)
                .all()
            )
            for event in events:
                if _stop_event.is_set():
                    break
                if event.next_retry_at and event.next_retry_at > now:
                    continue
                _process_event(event, session)
        except Exception:
            logger.exception("Error en ciclo del Outbox Publisher")
            try:
                session.rollback()
            except Exception:
                pass
        finally:
            try:
                session.close()
            except Exception:
                pass
        _stop_event.wait(POLL_INTERVAL)
    logger.info("Outbox Publisher detenido")


def start_outbox_publisher():
    global _publisher_thread
    _stop_event.clear()
    _publisher_thread = threading.Thread(target=_run)
    _publisher_thread.start()
    logger.info("Hilo del Outbox Publisher iniciado")


def stop_outbox_publisher():
    logger.info("Deteniendo Outbox Publisher...")
    _stop_event.set()
    if _publisher_thread:
        _publisher_thread.join(timeout=15)
    logger.info("Outbox Publisher detenido")
