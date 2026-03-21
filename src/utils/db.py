from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from config import DATABASE_URL
from src.utils.logger import get_logger

logger = get_logger("db")

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    pool_recycle=3600,
    pool_pre_ping=True,   
    echo=False
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

def get_session() -> Session:
    """Return a new database session."""
    return SessionLocal()

def test_connection() -> bool:
    """Verify the DB is reachable before starting the pipeline."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False