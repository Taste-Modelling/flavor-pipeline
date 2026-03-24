"""Tier 1 to Tier 2 molecule merger with multi-source attribution."""

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import SourceMetadata, Tier1Molecule
from flavor_pipeline.schemas.tier2 import MergeMetadata, Tier2Molecule

# Fields that contain lists of values (synonyms, descriptors)
# These should be unioned rather than compared for equality
LIST_VALUE_FIELDS = frozenset(
    {
        "synonyms",
        "flavor_descriptors",
        "odor_descriptors",
        "taste_descriptors",
    }
)

# All AVO fields on Tier1Molecule (excluding molecule_id, metadata, extra)
AVO_FIELDS = [
    "pubchem_cid",
    "cas",
    "smiles",
    "inchi",
    "inchi_key",
    "name",
    "iupac_name",
    "synonyms",
    "flavor_descriptors",
    "odor_descriptors",
    "taste_descriptors",
    "molecular_weight",
    "molecular_formula",
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


class Tier1Merger:
    """Merges Tier 1 molecules by molecule_id into Tier 2 molecules."""

    def __init__(self, pipeline_version: str = "1.0.0"):
        self.pipeline_version = pipeline_version

    def merge_all(self, molecules: list[Tier1Molecule]) -> list[Tier2Molecule]:
        """Merge all molecules by molecule_id.

        Args:
            molecules: List of Tier1Molecule objects to merge

        Returns:
            List of Tier2Molecule objects with merged data
        """
        # Group by molecule_id
        groups: dict[str, list[Tier1Molecule]] = defaultdict(list)
        for mol in molecules:
            groups[mol.molecule_id].append(mol)

        # Merge each group
        merged = []
        for mol_id, group in groups.items():
            merged.append(self._merge_group(mol_id, group))

        return merged

    def _merge_group(self, mol_id: str, molecules: list[Tier1Molecule]) -> Tier2Molecule:
        """Merge a group of molecules with the same ID.

        Args:
            mol_id: The shared molecule_id
            molecules: List of Tier1Molecule objects to merge

        Returns:
            A single Tier2Molecule with merged data
        """
        # Combine all _sources dicts
        combined_sources: dict[str, SourceMetadata] = {}
        for mol in molecules:
            combined_sources.update(mol.sources)

        # Track conflict count
        conflict_count = 0

        # Merge each AVO field
        merged_fields: dict[str, list[AttributedValue] | None] = {}
        for field_name in AVO_FIELDS:
            avos = []
            for mol in molecules:
                avo = getattr(mol, field_name)
                if avo is not None:
                    avos.append(avo)

            if not avos:
                merged_fields[field_name] = None
            else:
                merged_avos, has_conflict = self._merge_attributed_values(avos, field_name)
                merged_fields[field_name] = merged_avos
                if has_conflict:
                    conflict_count += 1

        # Merge extra fields
        merged_extra: dict[str, list[AttributedValue]] = defaultdict(list)
        for mol in molecules:
            for key, avo in mol.extra.items():
                merged_extra[key].append(avo)

        # Consolidate extra fields
        final_extra: dict[str, list[AttributedValue]] = {}
        for key, avos in merged_extra.items():
            merged_avos, has_conflict = self._merge_attributed_values(avos, key)
            final_extra[key] = merged_avos
            if has_conflict:
                conflict_count += 1

        # Create merge metadata
        merge_metadata = MergeMetadata(
            merged_at=datetime.now(UTC),
            pipeline_version=self.pipeline_version,
            source_count=len(combined_sources),
            conflict_count=conflict_count,
        )

        return Tier2Molecule(
            molecule_id=mol_id,
            _merge_metadata=merge_metadata,
            _sources=combined_sources,
            extra=final_extra if final_extra else {},
            **merged_fields,
        )

    def _merge_attributed_values(
        self, avos: list[AttributedValue], field_name: str
    ) -> tuple[list[AttributedValue], bool]:
        """Merge a list of AttributedValues.

        For list fields (synonyms, descriptors): union all values, combine sources.
        For scalar fields: group by normalized value, combine sources for identical values.

        Args:
            avos: List of AttributedValues to merge
            field_name: Name of the field (to determine if it's a list field)

        Returns:
            Tuple of (merged AVOs, has_conflict)
        """
        if not avos:
            return [], False

        is_list_field = field_name in LIST_VALUE_FIELDS

        if is_list_field:
            return self._merge_list_field(avos)
        else:
            return self._merge_scalar_field(avos)

    def _merge_list_field(self, avos: list[AttributedValue]) -> tuple[list[AttributedValue], bool]:
        """Merge list-valued fields by unioning values.

        Each unique value in the list becomes an entry with combined sources.
        """
        # Map normalized value -> (original value, sources, unit, conditions)
        value_map: dict[Any, dict] = {}

        for avo in avos:
            values = avo.value if isinstance(avo.value, list) else [avo.value]
            for val in values:
                normalized = _normalize_value(val)
                if normalized not in value_map:
                    value_map[normalized] = {
                        "value": val,
                        "sources": set(avo.sources),
                        "unit": avo.unit,
                        "conditions": avo.conditions,
                    }
                else:
                    value_map[normalized]["sources"].update(avo.sources)

        # Create merged AVOs - one per unique value
        merged = []
        for data in value_map.values():
            merged.append(
                AttributedValue(
                    value=data["value"],
                    sources=sorted(data["sources"]),
                    unit=data["unit"],
                    conditions=data["conditions"],
                )
            )

        # No conflict for list fields - we're unioning
        return merged, False

    def _merge_scalar_field(
        self, avos: list[AttributedValue]
    ) -> tuple[list[AttributedValue], bool]:
        """Merge scalar-valued fields by grouping identical values.

        Identical values (after normalization) are combined into a single AVO
        with merged sources. Different values are kept as separate AVOs.
        """
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
