"""Tier 1 to Tier 2 food merger with multi-source attribution."""

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.food import (
    FoodMergeMetadata,
    MoleculeComposition,
    Tier1Food,
    Tier2Food,
)
from flavor_pipeline.schemas.tier1 import SourceMetadata

# All AVO fields on Tier1Food (excluding food_id, metadata, extra, composition)
AVO_FIELDS = [
    "name",
    "scientific_name",
    "description",
    "category",
    "subcategory",
]


def _normalize_value(value: Any) -> Any:
    """Normalize a value for comparison.

    - Strings: lowercase and strip whitespace
    - Floats: round to 2 decimal places
    - Lists: sort and normalize each element
    - Other: return as-is
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value.lower().strip()
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, list):
        return tuple(sorted(_normalize_value(v) for v in value))
    return value


class Tier1FoodMerger:
    """Merges Tier 1 foods by food_id into Tier 2 foods."""

    def __init__(self, pipeline_version: str = "1.0.0"):
        self.pipeline_version = pipeline_version

    def merge_all(self, foods: list[Tier1Food]) -> list[Tier2Food]:
        """Merge all foods by food_id.

        Args:
            foods: List of Tier1Food objects to merge

        Returns:
            List of Tier2Food objects with merged data
        """
        # Group by food_id
        groups: dict[str, list[Tier1Food]] = defaultdict(list)
        for food in foods:
            groups[food.food_id].append(food)

        # Merge each group
        merged = []
        for food_id, group in groups.items():
            merged.append(self._merge_group(food_id, group))

        return merged

    def _merge_group(self, food_id: str, foods: list[Tier1Food]) -> Tier2Food:
        """Merge a group of foods with the same ID.

        Args:
            food_id: The shared food_id
            foods: List of Tier1Food objects to merge

        Returns:
            A single Tier2Food with merged data
        """
        # Combine all _sources dicts
        combined_sources: dict[str, SourceMetadata] = {}
        for food in foods:
            combined_sources.update(food.sources)

        # Track conflict count
        conflict_count = 0

        # Merge each AVO field
        merged_fields: dict[str, list[AttributedValue] | None] = {}
        for field_name in AVO_FIELDS:
            avos = []
            for food in foods:
                avo = getattr(food, field_name)
                if avo is not None:
                    avos.append(avo)

            if not avos:
                merged_fields[field_name] = None
            else:
                merged_avos, has_conflict = self._merge_attributed_values(avos)
                merged_fields[field_name] = merged_avos
                if has_conflict:
                    conflict_count += 1

        # Merge composition by molecule_id
        merged_composition = self._merge_composition(foods)

        # Merge extra fields
        merged_extra: dict[str, list[AttributedValue]] = defaultdict(list)
        for food in foods:
            for key, avo in food.extra.items():
                merged_extra[key].append(avo)

        # Consolidate extra fields
        final_extra: dict[str, list[AttributedValue]] = {}
        for key, avos in merged_extra.items():
            merged_avos, has_conflict = self._merge_attributed_values(avos)
            final_extra[key] = merged_avos
            if has_conflict:
                conflict_count += 1

        # Create merge metadata
        merge_metadata = FoodMergeMetadata(
            merged_at=datetime.now(UTC),
            pipeline_version=self.pipeline_version,
            source_count=len(combined_sources),
            conflict_count=conflict_count,
            molecule_count=len(merged_composition),
        )

        return Tier2Food(
            food_id=food_id,
            _merge_metadata=merge_metadata,
            _sources=combined_sources,
            composition=merged_composition,
            extra=final_extra if final_extra else {},
            **merged_fields,
        )

    def _merge_composition(
        self, foods: list[Tier1Food]
    ) -> dict[str, list[MoleculeComposition]]:
        """Merge molecular composition from multiple food sources.

        Groups by molecule_id, keeping all composition entries from different sources.
        """
        composition: dict[str, list[MoleculeComposition]] = defaultdict(list)

        for food in foods:
            for mc in food.composition:
                composition[mc.molecule_id].append(mc)

        return dict(composition)

    def _merge_attributed_values(
        self, avos: list[AttributedValue]
    ) -> tuple[list[AttributedValue], bool]:
        """Merge a list of AttributedValues.

        Groups by normalized value, combines sources for identical values.

        Args:
            avos: List of AttributedValues to merge

        Returns:
            Tuple of (merged AVOs, has_conflict)
        """
        if not avos:
            return [], False

        # Group by normalized value
        value_groups: dict[Any, list[AttributedValue]] = defaultdict(list)
        for avo in avos:
            normalized = _normalize_value(avo.value)
            value_groups[normalized].append(avo)

        # Create merged AVOs - one per unique normalized value
        merged = []
        for group in value_groups.values():
            # Combine sources from all AVOs with this normalized value
            all_sources: set[str] = set()
            for avo in group:
                all_sources.update(avo.sources)

            # Use the first AVO's value (original, not normalized) and metadata
            first_avo = group[0]
            merged.append(
                AttributedValue(
                    value=first_avo.value,
                    sources=sorted(all_sources),
                    unit=first_avo.unit,
                    conditions=first_avo.conditions,
                )
            )

        # Conflict if there are multiple distinct normalized values
        has_conflict = len(merged) > 1

        return merged, has_conflict
