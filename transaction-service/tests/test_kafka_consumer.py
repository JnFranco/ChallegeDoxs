import json
import uuid
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.models import Transaction


def _create_pending(session, value=100):
    tx_id = uuid.uuid4()
    tx = Transaction(
        transaction_external_id=tx_id,
        account_external_id_debit=uuid.uuid4(),
        account_external_id_credit=uuid.uuid4(),
        transfer_type_id=1,
        value=Decimal(str(value)),
        status="pending",
    )
    session.add(tx)
    session.commit()
    return tx_id


def _make_message(tx_id, status):
    msg = MagicMock()
    payload = {
        "transactionExternalId": str(tx_id),
        "status": status,
        "occurredAt": "2026-08-18T15:30:00+00:00",
    }
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    return msg


def _run_with_session(db_session, fn, *args, **kwargs):
    """Ejecuta process_decision usando el db_session del test."""
    with patch("app.kafka_consumer.SessionLocal", return_value=db_session):
        return fn(*args, **kwargs)


def test_approved_updates_pending(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    msg = _make_message(tx_id, "approved")

    result = _run_with_session(db_session, process_decision, msg)
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "approved"


def test_rejected_updates_pending(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    msg = _make_message(tx_id, "rejected")

    result = _run_with_session(db_session, process_decision, msg)
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "rejected"


def test_approved_on_already_approved(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    _run_with_session(db_session, process_decision, _make_message(tx_id, "approved"))

    result = _run_with_session(db_session, process_decision, _make_message(tx_id, "approved"))
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "approved"


def test_rejected_on_already_rejected(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    _run_with_session(db_session, process_decision, _make_message(tx_id, "rejected"))

    result = _run_with_session(db_session, process_decision, _make_message(tx_id, "rejected"))
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "rejected"


def test_opposite_decision_ignored(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    _run_with_session(db_session, process_decision, _make_message(tx_id, "approved"))

    result = _run_with_session(db_session, process_decision, _make_message(tx_id, "rejected"))
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "approved"


def test_unknown_transaction(db_session):
    from app.kafka_consumer import process_decision

    fake_id = uuid.uuid4()
    msg = _make_message(fake_id, "approved")

    result = _run_with_session(db_session, process_decision, msg)
    assert result is True


def test_invalid_uuid(db_session):
    from app.kafka_consumer import process_decision

    msg = MagicMock()
    payload = {
        "transactionExternalId": "not-a-uuid",
        "status": "approved",
    }
    msg.value.return_value = json.dumps(payload).encode("utf-8")

    with pytest.raises(ValueError, match="transactionExternalId inválido"):
        _run_with_session(db_session, process_decision, msg)


def test_invalid_status(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    msg = _make_message(tx_id, "pending")

    with pytest.raises(ValueError, match="status inválido"):
        _run_with_session(db_session, process_decision, msg)


def test_duplicate_message(db_session):
    from app.kafka_consumer import process_decision

    tx_id = _create_pending(db_session)
    msg = _make_message(tx_id, "approved")

    _run_with_session(db_session, process_decision, msg)
    result = _run_with_session(db_session, process_decision, msg)
    assert result is True

    tx = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert tx.status == "approved"


def test_missing_fields():
    from app.kafka_consumer import process_decision

    msg = MagicMock()
    msg.value.return_value = json.dumps({"status": "approved"}).encode("utf-8")

    with pytest.raises(ValueError, match="Faltan campos obligatorios"):
        process_decision(msg)
