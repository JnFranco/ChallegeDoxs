from sqlalchemy import select

from app.database import SessionLocal, engine
from app.models import Base, TransferType


def init_db():
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        exists = session.execute(
            select(TransferType).where(TransferType.id == 1)
        ).scalar_one_or_none()

        if not exists:
            session.add(TransferType(id=1, name="transfer"))
            session.commit()
    finally:
        session.close()
