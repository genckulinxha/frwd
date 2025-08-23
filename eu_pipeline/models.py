from datetime import datetime
from sqlalchemy import Text, Column, Integer, String, Date, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base

EUBase = declarative_base()


class EULaw(EUBase):
    __tablename__ = "eu_laws"

    id = Column(Integer, primary_key=True)
    celex_id = Column(String, nullable=False, unique=True)
    title = Column(String, nullable=True)
    law_type = Column(String, nullable=True)
    year = Column(String, nullable=True)
    document_number = Column(String, nullable=True)
    publish_date = Column(Date, nullable=True)

    detail_url = Column(String, nullable=False)

    pdf_downloaded = Column(Boolean, default=False)
    pdf_path = Column(String, nullable=True)

    last_seen_at = Column(DateTime, nullable=True)
    processed_at = Column(DateTime, nullable=True)
    unprocessed = Column(Boolean, default=True)

    pdf_text = Column(Text, nullable=True)
    pdf_text_extracted_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)


class EULawRelation(EUBase):
    __tablename__ = "eu_law_relations"

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("eu_laws.id"), nullable=False)
    target_id = Column(Integer, ForeignKey("eu_laws.id"), nullable=False)
    relation_type = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("source_id", "target_id", "relation_type", name="uq_eu_relation"),
    ) 