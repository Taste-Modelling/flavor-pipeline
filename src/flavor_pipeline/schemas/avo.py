"""Attributed Value Object for source lineage tracking."""

from typing import Any

from pydantic import BaseModel


class AttributedValue(BaseModel):
    """Wraps a value with source lineage.

    The AttributedValue (AVO) pattern allows tracking which data sources
    contributed to each field value, enabling provenance tracking and
    conflict resolution during data consolidation.
    """

    value: Any
    unit: str | None = None
    sources: list[str]  # Source IDs that provided this value
    conditions: dict[str, Any] | None = None

    model_config = {"frozen": True}
