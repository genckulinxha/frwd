import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from pipeline.fetch_links import fetch_category_links
from models import Law
from db import get_session
from pipeline.categories import CATEGORY_URLS

logger = logging.getLogger(__name__)

def discover_laws():
    session: Session = get_session()
    
    try:
        total_processed = 0
        total_new = 0
        total_updated = 0
        total_errors = 0

        for category, url in CATEGORY_URLS.items():
            logger.info(f"🔍 Discovering laws in category: {category}")
            
            try:
                links = fetch_category_links(category, url)
                logger.info(f"   → Fetched {len(links)} links")
                
                if not links:
                    logger.warning(f"   ⚠️ No links found for category: {category}")
                    continue
                
                # Deduplicate links by act_id to prevent duplicates within the same category
                unique_links = {}
                for link in links:
                    act_id = link.get("act_id")
                    if act_id and act_id not in unique_links:
                        unique_links[act_id] = link
                    elif act_id in unique_links:
                        logger.debug(f"   ⚠️ Duplicate act_id {act_id} found in category {category}, skipping")
                
                links = list(unique_links.values())
                logger.info(f"   → After deduplication: {len(links)} unique links")
                    
            except Exception as e:
                logger.error(f"❌ Failed to fetch links for {category}: {e}")
                logger.exception(f"Detailed error for category {category}:")
                total_errors += 1
                continue

            # Process links in batches to avoid memory issues
            batch_size = 100
            for i in range(0, len(links), batch_size):
                batch = links[i:i + batch_size]
                batch_new = 0
                batch_updated = 0
                batch_errors = 0
                
                logger.info(f"   📦 Processing batch {i//batch_size + 1} ({len(batch)} items)")
                
                # Additional deduplication within batch (should not be needed, but extra safety)
                batch_act_ids = set()
                processed_in_batch = set()
                
                for entry in batch:
                    try:
                        act_id = entry["act_id"]
                        detail_url = entry["detail_url"]
                        
                        if not act_id or not detail_url:
                            logger.warning(f"   ⚠️ Skipping invalid entry: {entry}")
                            batch_errors += 1
                            continue
                        
                        # Check for duplicates within this batch
                        if act_id in processed_in_batch:
                            logger.warning(f"   ⚠️ Duplicate act_id {act_id} within batch, skipping")
                            batch_errors += 1
                            continue
                        
                        processed_in_batch.add(act_id)

                        # Check if law already exists in database
                        law = session.query(Law).filter_by(act_id=act_id).first()
                        now = datetime.utcnow()

                        if law:
                            # Update existing law
                            law.last_seen_at = now
                            # Update detail_url if it has changed
                            if law.detail_url != detail_url:
                                law.detail_url = detail_url
                                logger.info(f"   🔄 Updated URL for existing law ActID={act_id}")
                            batch_updated += 1
                            logger.debug(f"   ✓ Updated timestamp for existing law ActID={act_id}")
                        else:
                            # Create new law
                            law = Law(
                                act_id=act_id,
                                category=category,
                                detail_url=detail_url,
                                last_seen_at=now,
                                unprocessed=True,
                            )
                            session.add(law)
                            batch_new += 1
                            logger.info(f"   [+] Discovered new law ActID={act_id}")
                            
                    except Exception as e:
                        logger.error(f"   ❌ Error processing entry {entry}: {e}")
                        batch_errors += 1
                        continue

                # Commit batch with proper error handling
                try:
                    session.commit()
                    logger.info(f"   ✅ Batch committed: {batch_new} new, {batch_updated} updated, {batch_errors} errors")
                    total_new += batch_new
                    total_updated += batch_updated
                    total_errors += batch_errors
                    total_processed += len(batch)
                    
                except IntegrityError as e:
                    logger.error(f"   ❌ Integrity error in batch: {e}")
                    session.rollback()
                    
                    # This should rarely happen now with deduplication, but handle it anyway
                    logger.info(f"   🔍 Processing batch individually due to integrity error...")
                    individual_new = 0
                    individual_updated = 0
                    individual_errors = 0
                    individual_processed = set()
                    
                    for entry in batch:
                        try:
                            act_id = entry["act_id"]
                            detail_url = entry["detail_url"]
                            
                            if not act_id or not detail_url:
                                continue
                            
                            # Skip if already processed in this individual recovery
                            if act_id in individual_processed:
                                logger.debug(f"   ⚠️ Skipping already processed act_id {act_id} in recovery")
                                continue
                            
                            individual_processed.add(act_id)
                                
                            law = session.query(Law).filter_by(act_id=act_id).first()
                            now = datetime.utcnow()

                            if law:
                                # Update existing law
                                law.last_seen_at = now
                                if law.detail_url != detail_url:
                                    law.detail_url = detail_url
                                individual_updated += 1
                                logger.debug(f"   ✓ Updated existing law ActID={act_id}")
                            else:
                                # Create new law
                                law = Law(
                                    act_id=act_id,
                                    category=category,
                                    detail_url=detail_url,
                                    last_seen_at=now,
                                    unprocessed=True,
                                )
                                session.add(law)
                                individual_new += 1
                                logger.debug(f"   [+] Created new law ActID={act_id}")
                                
                            session.commit()
                            
                        except IntegrityError as ie:
                            logger.warning(f"   ⚠️ Duplicate key for ActID={act_id}: {ie}")
                            session.rollback()
                            individual_errors += 1
                            
                            # Try to update existing record
                            try:
                                law = session.query(Law).filter_by(act_id=act_id).first()
                                if law:
                                    law.last_seen_at = now
                                    if law.detail_url != detail_url:
                                        law.detail_url = detail_url
                                    session.commit()
                                    individual_updated += 1
                                    logger.info(f"   ✓ Updated existing law ActID={act_id} after conflict")
                                else:
                                    logger.warning(f"   ⚠️ Could not find law ActID={act_id} after duplicate key error")
                            except Exception as update_e:
                                logger.error(f"   ❌ Failed to update existing law ActID={act_id}: {update_e}")
                                session.rollback()
                                
                        except Exception as e:
                            logger.error(f"   ❌ Individual processing error for ActID={act_id}: {e}")
                            session.rollback()
                            individual_errors += 1

                    logger.info(f"   ✅ Individual processing: {individual_new} new, {individual_updated} updated, {individual_errors} errors")
                    total_new += individual_new
                    total_updated += individual_updated
                    total_errors += individual_errors
                    total_processed += len(batch)
                    
                except Exception as e:
                    logger.error(f"   ❌ Unexpected error during batch commit: {e}")
                    session.rollback()
                    total_errors += len(batch)

        logger.info(f"✅ Law discovery complete: {total_processed} processed, {total_new} new, {total_updated} updated, {total_errors} errors")

    except Exception as e:
        logger.error(f"❌ Fatal error in discover_laws: {e}")
        logger.exception("Detailed error:")
        session.rollback()
        raise
    finally:
        try:
            session.close()
        except Exception as e:
            logger.error(f"❌ Error closing session: {e}")
