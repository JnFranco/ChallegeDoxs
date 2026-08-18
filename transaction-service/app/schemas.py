from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, field_validator


class TransactionCreate(BaseModel):
    accountExternalIdDebit: UUID
    accountExternalIdCredit: UUID
    transferTypeId: int
    value: Decimal

    @field_validator("value")
    @classmethod
    def value_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("value debe ser mayor que 0")
        return v


class TransactionTypeOut(BaseModel):
    name: str


class TransactionStatusOut(BaseModel):
    name: str


class TransactionCreatedResponse(BaseModel):
    transactionExternalId: UUID


class TransactionResponse(BaseModel):
    transactionExternalId: UUID
    transactionType: TransactionTypeOut
    transactionStatus: TransactionStatusOut
    value: Decimal
    createdAt: datetime
