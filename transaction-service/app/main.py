import json
import logging
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException
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

app = FastAPI(title="Transaction Service")


@app.on_event("startup")
def startup():
    init_db()
    start_consumer_thread()
    start_outbox_publisher()


@app.on_event("shutdown")
def shutdown():
    stop_outbox_publisher()
    stop_consumer()


@app.post("/transactions", status_code=201, response_model=TransactionCreatedResponse)
def create_transaction(body: TransactionCreate, db: Session = Depends(get_db)):
    # Las cuentas débito y crédito deben ser distintas.
    if body.accountExternalIdDebit == body.accountExternalIdCredit:
        raise HTTPException(
            status_code=422,
            detail="accountExternalIdDebit y accountExternalIdCredit deben ser distintos",
        )

    # El tipo de transferencia debe existir en la tabla transfer_types.
    transfer_type = db.get(TransferType, body.transferTypeId)
    if not transfer_type:
        raise HTTPException(
            status_code=422,
            detail=f"transferTypeId {body.transferTypeId} no existe",
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


@app.get("/transactions/{transaction_external_id}", response_model=TransactionResponse)
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


@app.get("/health")
def health():
    return {"status": "ok"}
