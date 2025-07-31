import os
import logging
import sys
from dataclasses import dataclass
from typing import Callable, Optional
from sqlalchemy import text
from db import init_db, get_session
from pipeline.discovery.discover_laws_v2 import discover_laws
from pipeline.detail.process_laws_v2 import process_unprocessed_laws
from pipeline.relations.backfill_relations_v2 import backfill_relations

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Phase:
    """Represents a single phase in the pipeline."""
    name: str
    description: str
    function: Callable
    icon: str


class Pipeline:
    """Manages the execution of all pipeline phases."""
    
    def __init__(self):
        self.session = None
        self.phases = [
            Phase("Discovery", "Discover laws", discover_laws, "📋"),
            Phase("Processing", "Process law metadata and PDFs", self._process_laws, "📄"),
            Phase("Relations", "Backfill law relations", self._backfill_relations, "🔗"),
        ]
    
    def _process_laws(self):
        """Wrapper for process_unprocessed_laws that handles session."""
        process_unprocessed_laws(self.session)
    
    def _backfill_relations(self):
        """Wrapper for backfill_relations that handles session."""
        backfill_relations(self.session)
    
    def setup(self):
        """Initialize database and create necessary directories."""
        logger.info("🚀 Initializing database and folders...")
        init_db()
        os.makedirs("data", exist_ok=True)
        
        self.session = get_session()
        self.session.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
    
    def run_phase(self, phase: Phase) -> bool:
        """Execute a single phase and return success status."""
        logger.info(f"{phase.icon} Phase: {phase.description}...")
        try:
            phase.function()
            logger.info(f"✅ {phase.name} phase completed successfully")
            return True
        except Exception as e:
            logger.error(f"❌ {phase.name} phase failed: {e}")
            logger.exception(f"{phase.name} phase detailed error:")
            return False
    
    def run(self):
        """Execute all phases in sequence."""
        success_count = 0
        
        for phase in self.phases:
            if self.run_phase(phase):
                success_count += 1
        
        # Log final results
        total_phases = len(self.phases)
        if success_count == total_phases:
            logger.info("✅ All phases completed successfully!")
        else:
            failed_count = total_phases - success_count
            logger.warning(f"⚠️ Pipeline completed with {failed_count} failed phase(s)")
    
    def cleanup(self):
        """Clean up database session and other resources."""
        if self.session:
            try:
                self.session.close()
                logger.info("🔒 Database session closed")
            except Exception as e:
                logger.error(f"❌ Error closing database session: {e}")


def main():
    """Main entry point for the pipeline."""
    pipeline = Pipeline()
    
    try:
        pipeline.setup()
        pipeline.run()
    except Exception as e:
        logger.error(f"❌ Critical error in pipeline: {e}")
        logger.exception("Pipeline detailed error:")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()
