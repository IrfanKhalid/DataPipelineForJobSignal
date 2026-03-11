from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.models import DatabaseConfig


def create_session_factory(config: DatabaseConfig) -> sessionmaker[Session]:
    """Create a SQLAlchemy session factory bound to the configured database."""
    engine = create_engine(
        config.url,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,  # detect stale connections
    )
    return sessionmaker(bind=engine)


@contextmanager
def get_session(factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    """Context manager that provides a transactional session scope."""
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
