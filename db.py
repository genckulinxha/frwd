import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from models import Base
from dotenv import load_dotenv
import time

logger = logging.getLogger(__name__)

load_dotenv()  # Load from .env file

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment")

# Use NullPool for CLI tools or Alembic, or use default pool for production
try:
    engine = create_engine(DATABASE_URL, echo=False, poolclass=NullPool)
    logger.info("✅ Database engine created successfully")
except Exception as e:
    logger.error(f"❌ Failed to create database engine: {e}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Create tables (for local dev only)."""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔄 Attempting to initialize database (attempt {attempt + 1}/{max_retries})...")
            Base.metadata.create_all(bind=engine)
            logger.info("✅ Database tables created successfully")
            return
        except OperationalError as e:
            logger.error(f"❌ Database connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ All database initialization attempts failed")
                raise
        except SQLAlchemyError as e:
            logger.error(f"❌ Database schema error: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error during database initialization: {e}")
            raise

def get_session():
    """Returns a new SQLAlchemy session."""
    try:
        session = SessionLocal()
        logger.debug("✅ Database session created")
        return session
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to create database session: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error creating database session: {e}")
        raise

def test_connection():
    """Test database connection."""
    try:
        session = get_session()
        session.execute(text("SELECT 1"))
        session.close()
        logger.info("✅ Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False
