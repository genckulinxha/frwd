import logging
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from models import Law, LawRelation
from pipeline.base import BasePipelineProcessor, ValidationMixin, PipelineError
from pipeline.config import CONFIG, RetryConfig, BatchConfig
from pipeline.utils import safe_strip

logger = logging.getLogger(__name__)

RELATION_TYPES = [
    "shfuqizon", "ndryshon", "ndryshohet",
    "plotëson", "plotësohet", "ndryshon pjesërisht", "shfuqizon pjesërisht"
]


class RelationsProcessor(BasePipelineProcessor, ValidationMixin):
    """Improved law relations processor."""
    
    @classmethod
    def get_model_class(cls):
        """Return the model class this processor works with."""
        return Law
    
    def get_retry_config(self) -> RetryConfig:
        return CONFIG.relations_retry
    
    def get_batch_config(self) -> BatchConfig:
        return CONFIG.relations_batch
    
    def get_items_to_process(self) -> List[Any]:
        """Get processed laws that need relation extraction."""
        return self.session.query(Law).filter_by(unprocessed=False).all()
    
    def process_single_item(self, item: Any) -> Dict[str, Any]:
        """Process relations for a single law."""
        law = item
        
        try:
            if not law.detail_url:
                logger.warning(f"No detail URL for ActID={law.act_id}")
                return {"status": "skipped", "act_id": law.act_id}
            
            relations_count = self._extract_and_store_relations(law)
            
            return {
                "status": "processed" if relations_count >= 0 else "error",
                "act_id": law.act_id,
                "relations_count": relations_count
            }
            
        except Exception as e:
            logger.error(f"Error processing relations for ActID={law.act_id}: {e}")
            return {"status": "error", "act_id": law.act_id, "error": str(e)}
    
    def _extract_and_store_relations(self, law: Law) -> int:
        """Extract and store relations for a law."""
        try:
            with self.get_http_client() as client:
                response = client.get(law.detail_url)
                soup = client.parse_html(response.text)
                
                # Find relations container
                container = soup.select_one("#MainContent_drNActRelated")
                if not container:
                    logger.debug(f"No relations container found for ActID={law.act_id}")
                    return 0
                
                # Extract relation boxes
                boxes = container.select("div.act_link_box_1")
                relations_count = 0
                
                for box in boxes:
                    try:
                        relation_created = self._process_relation_box(law, box)
                        if relation_created:
                            relations_count += 1
                    except Exception as e:
                        logger.warning(f"Error processing relation box for ActID={law.act_id}: {e}")
                        continue
                
                logger.debug(f"Processed {relations_count} relations for ActID={law.act_id}")
                return relations_count
                
        except Exception as e:
            logger.error(f"Error extracting relations for ActID={law.act_id}: {e}")
            return -1
    
    def _process_relation_box(self, source_law: Law, box) -> bool:
        """Process a single relation box."""
        try:
            # Extract link
            link = box.select_one("a[href*='ActID=']")
            if not link:
                return False
            
            href = link.get("href")
            if not href:
                return False
            
            # Extract target act_id
            target_act_id = self._extract_act_id_from_url(href)
            if not target_act_id:
                return False
            
            # Get or create target law
            target_law = self._get_or_create_target_law(target_act_id, href, source_law.category)
            if not target_law:
                return False
            
            # Extract relation type
            relation_type = self._extract_relation_type(box)
            
            # Create relation if it doesn't exist
            return self._create_relation(source_law, target_law, relation_type)
            
        except Exception as e:
            logger.warning(f"Error processing relation box: {e}")
            return False
    
    def _extract_act_id_from_url(self, href: str) -> Optional[int]:
        """Extract act_id from URL."""
        try:
            if "ActID=" in href:
                act_id_str = href.split("ActID=")[1].split("&")[0]
                return self.validate_act_id(act_id_str)
        except Exception:
            pass
        return None
    
    def _get_or_create_target_law(self, act_id: int, href: str, category: str) -> Optional[Law]:
        """Get existing law or create new one."""
        try:
            # Check if law exists
            target_law = self.session.query(Law).filter_by(act_id=act_id).first()
            
            if target_law:
                return target_law
            
            # Create new law
            full_url = urljoin(CONFIG.base_url, href)
            target_law = Law(
                act_id=act_id,
                detail_url=full_url,
                category=category,
                unprocessed=True
            )
            
            self.session.add(target_law)
            self.session.flush()  # Get the ID
            
            logger.debug(f"Created new target law ActID={act_id}")
            return target_law
            
        except Exception as e:
            logger.error(f"Error creating target law ActID={act_id}: {e}")
            return None
    
    def _extract_relation_type(self, box) -> str:
        """Extract relation type from relation box."""
        try:
            # Look for relation type indicators in the box text
            box_text = self.sanitize_text(safe_strip(box)).lower()
            
            for relation_type in RELATION_TYPES:
                if relation_type.lower() in box_text:
                    return relation_type
            
            # Default relation type
            return "related"
            
        except Exception as e:
            logger.warning(f"Error extracting relation type: {e}")
            return "related"
    
    def _create_relation(self, source_law: Law, target_law: Law, relation_type: str) -> bool:
        """Create a law relation if it doesn't exist."""
        try:
            # Check if relation already exists
            existing_relation = self.session.query(LawRelation).filter_by(
                source_id=source_law.id,
                target_id=target_law.id,
                relation_type=relation_type
            ).first()
            
            if existing_relation:
                logger.debug(f"Relation already exists: {source_law.act_id} -> {target_law.act_id}")
                return False
            
            # Create new relation
            relation = LawRelation(
                source_id=source_law.id,
                target_id=target_law.id,
                relation_type=relation_type
            )
            
            self.session.add(relation)
            
            logger.debug(f"Created relation: {source_law.act_id} -{relation_type}-> {target_law.act_id}")
            return True
            
        except IntegrityError as e:
            logger.warning(f"Integrity error creating relation: {e}")
            self.session.rollback()
            return False
        except Exception as e:
            logger.error(f"Error creating relation: {e}")
            self.session.rollback()
            return False


def backfill_relations(session: Session, batch_size: int = 50):
    """Main entry point for relation backfill."""
    try:
        processor = RelationsProcessor(session)
        processor.run()
    finally:
        pass  # Session managed by caller 