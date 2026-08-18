import enum
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database import Base


class TransactionStatus(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class TransferType(Base):
    __tablename__ = "transfer_types"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_external_id = Column(
        UUID(as_uuid=True), unique=True, nullable=False, index=True
    )
    account_external_id_debit = Column(UUID(as_uuid=True), nullable=False)
    account_external_id_credit = Column(UUID(as_uuid=True), nullable=False)
    transfer_type_id = Column(
        Integer, ForeignKey("transfer_types.id"), nullable=False
    )
    value = Column(Numeric(12, 2), nullable=False)
    status = Column(
        String(20),
        nullable=False,
        default=TransactionStatus.PENDING.value,
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    transfer_type = relationship("TransferType")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = Column(
        UUID(as_uuid=True), unique=True, nullable=False, index=True
    )
    event_type = Column(String(50), nullable=False)
    aggregate_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    published_at = Column(DateTime(timezone=True), nullable=True)
    attempts = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
