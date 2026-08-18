import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.fraud import evaluate_fraud


def test_value_below_1000_approved():
    assert evaluate_fraud(Decimal("500")) == "approved"


def test_value_equal_1000_approved():
    assert evaluate_fraud(Decimal("1000")) == "approved"


def test_value_above_1000_rejected():
    assert evaluate_fraud(Decimal("1001")) == "rejected"


def test_value_zero_approved():
    assert evaluate_fraud(Decimal("0")) == "approved"


def test_value_999_approved():
    assert evaluate_fraud(Decimal("999")) == "approved"


def test_value_1001_rejected():
    assert evaluate_fraud(Decimal("1001")) == "rejected"


def test_value_2000_rejected():
    assert evaluate_fraud(Decimal("2000")) == "rejected"


def test_value_decimal_approved():
    assert evaluate_fraud(Decimal("999.99")) == "approved"


def test_value_decimal_rejected():
    assert evaluate_fraud(Decimal("1000.01")) == "rejected"


def test_publish_decision_uses_correct_topic():
    from app.producer import publish_fraud_decision

    with patch("app.producer.producer") as mock_producer:
        publish_fraud_decision("test-uuid", "approved")
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[1]["topic"] == "fraud.decisions"


def test_publish_decision_payload():
    from app.producer import publish_fraud_decision

    with patch("app.producer.producer") as mock_producer:
        publish_fraud_decision("test-uuid", "rejected")
        call_kwargs = mock_producer.produce.call_args
        payload = json.loads(call_kwargs[1]["value"])

        assert payload["transactionExternalId"] == "test-uuid"
        assert payload["status"] == "rejected"
        assert "occurredAt" in payload


def test_publish_decision_uses_external_id_as_key():
    from app.producer import publish_fraud_decision

    with patch("app.producer.producer") as mock_producer:
        publish_fraud_decision("my-uuid", "approved")
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[1]["key"] == "my-uuid"


def test_process_transaction_approved():
    from app.consumer import process_transaction

    msg = MagicMock()
    msg.value.return_value = json.dumps({
        "transactionExternalId": "uuid-123",
        "value": 100,
    }).encode("utf-8")

    with patch("app.consumer.publish_fraud_decision") as mock_publish:
        process_transaction(msg)
        mock_publish.assert_called_once_with("uuid-123", "approved")


def test_process_transaction_rejected():
    from app.consumer import process_transaction

    msg = MagicMock()
    msg.value.return_value = json.dumps({
        "transactionExternalId": "uuid-456",
        "value": 1500,
    }).encode("utf-8")

    with patch("app.consumer.publish_fraud_decision") as mock_publish:
        process_transaction(msg)
        mock_publish.assert_called_once_with("uuid-456", "rejected")
