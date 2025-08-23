import os
import logging
import sys
from dataclasses import dataclass
from typing import Callable
from sqlalchemy import text

from eu_pipeline.db import init_db, get_session
from eu_pipeline.discovery import discover_eu_laws
from eu_pipeline.detail import process_unprocessed_eu_laws
from eu_pipeline.relations import backfill_eu_relations

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Phase:
    name: str
    description: str
    function: Callable
    icon: str


class EUPipeline:
    def __init__(self):
        self.session = None
        self.phases = [
            Phase("EU Discovery", "Discover EU laws", discover_eu_laws, "🧭"),
            Phase("EU Processing", "Process EU law metadata and PDFs", self._process_laws, "📄"),
            Phase("EU Relations", "Backfill EU law relations", self._backfill_relations, "🔗"),
        ]

    def _process_laws(self):
        process_unprocessed_eu_laws(self.session)

    def _backfill_relations(self):
        backfill_eu_relations(self.session)

    def setup(self):
        logger.info("🚀 Initializing EU database and folders...")
        init_db()
        os.makedirs("data_eu", exist_ok=True)
        self.session = get_session()
        self.session.execute(text("SELECT 1"))
        logger.info("✅ EU Database connection successful")

    def run_phase(self, phase: Phase) -> bool:
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
        success_count = 0
        for phase in self.phases:
            if self.run_phase(phase):
                success_count += 1
        total_phases = len(self.phases)
        if success_count == total_phases:
            logger.info("✅ All EU phases completed successfully!")
        else:
            failed_count = total_phases - success_count
            logger.warning(f"⚠️ EU Pipeline completed with {failed_count} failed phase(s)")

    def cleanup(self):
        if self.session:
            try:
                self.session.close()
                logger.info("🔒 EU Database session closed")
            except Exception as e:
                logger.error(f"❌ Error closing EU database session: {e}")


def main():
    pipeline = EUPipeline()
    try:
        pipeline.setup()
        pipeline.run()
    except Exception as e:
        logger.error(f"❌ Critical error in EU pipeline: {e}")
        logger.exception("EU Pipeline detailed error:")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main() 