from datetime import datetime
from sqlalchemy import (
    Text, Column, Integer, String, Date, DateTime, Boolean, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Law(Base):
    __tablename__ = "laws"
    id = Column(Integer, primary_key=True)
    act_id = Column(Integer, nullable=False, unique=True)  # unique across categories
    title = Column(String, nullable=True)
    law_type = Column(String, nullable=True)
    institution = Column(String, nullable=True)
    law_number = Column(String, nullable=True)
    gazette_number = Column(String, nullable=True)
    publish_date = Column(Date, nullable=True)

    category = Column(String, nullable=False)
    detail_url = Column(String, nullable=False)

    pdf_downloaded = Column(Boolean, default=False)
    pdf_path = Column(String, nullable=True)

    last_seen_at = Column(DateTime, nullable=True)     # used in discovery phase
    processed_at = Column(DateTime, nullable=True)      # used in detail parsing phase
    unprocessed = Column(Boolean, default=True)         # used to track processing state

    pdf_text = Column(Text, nullable=True)
    pdf_text_extracted_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relations
    related_to = relationship(
        "LawRelation",
        back_populates="source",
        foreign_keys="LawRelation.source_id",
        cascade="all, delete-orphan"
    )

    related_from = relationship(
        "LawRelation",
        back_populates="target",
        foreign_keys="LawRelation.target_id",
        cascade="all, delete-orphan"
    )


class LawRelation(Base):
    __tablename__ = "law_relations"
    id = Column(Integer, primary_key=True)

    source_id = Column(Integer, ForeignKey("laws.id", ondelete="CASCADE"))
    target_id = Column(Integer, ForeignKey("laws.id", ondelete="CASCADE"))
    relation_type = Column(String, nullable=False)  # e.g. 'shfuqizon', 'ndryshon'
    comment = Column(String, nullable=True)

    source = relationship("Law", back_populates="related_to", foreign_keys=[source_id])
    target = relationship("Law", back_populates="related_from", foreign_keys=[target_id])

    __table_args__ = (UniqueConstraint("source_id", "target_id", "relation_type", name="uq_relation"),)
