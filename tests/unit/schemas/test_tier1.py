"""Tests for Tier 1 molecule schema."""

from datetime import UTC, datetime

from flavor_pipeline.schemas.avo import AttributedValue
from flavor_pipeline.schemas.tier1 import IngestMetadata, SourceMetadata, Tier1Molecule


class TestSourceMetadata:
    """Tests for SourceMetadata."""

    def test_source_metadata_creation(self):
        """Test basic source metadata creation."""
        meta = SourceMetadata(
            name="flavordb2",
            version="2024.1",
            url="https://cosylab.iiitd.edu.in/flavordb2/",
            retrieved_at=datetime(2024, 1, 15, 10, 30, 0),
            parser_version="0.1.0",
        )
        assert meta.name == "flavordb2"
        assert meta.version == "2024.1"
        assert meta.parser_version == "0.1.0"

    def test_source_metadata_optional_url(self):
        """Test source metadata with optional URL."""
        meta = SourceMetadata(
            name="internal_db",
            version="1.0",
            retrieved_at=datetime.now(),
            parser_version="0.1.0",
        )
        assert meta.url is None


class TestIngestMetadata:
    """Tests for IngestMetadata."""

    def test_ingest_metadata_auto_timestamp(self):
        """Test that ingested_at defaults to current time."""
        before = datetime.now(UTC)
        meta = IngestMetadata(pipeline_version="0.1.0")
        after = datetime.now(UTC)

        assert before <= meta.ingested_at <= after

    def test_ingest_metadata_explicit_timestamp(self):
        """Test explicit ingested_at timestamp."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        meta = IngestMetadata(
            ingested_at=timestamp,
            pipeline_version="0.1.0",
        )
        assert meta.ingested_at == timestamp


class TestTier1Molecule:
    """Tests for Tier1Molecule."""

    def test_tier1_minimal_molecule(self):
        """Test creating a molecule with only required fields."""
        mol = Tier1Molecule(
            molecule_id="pubchem:123",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
        )
        assert mol.molecule_id == "pubchem:123"
        assert mol.pubchem_cid is None
        assert mol.smiles is None

    def test_tier1_full_molecule(self):
        """Test creating a molecule with all fields populated."""
        source_meta = SourceMetadata(
            name="flavordb2",
            version="2024.1",
            url="https://cosylab.iiitd.edu.in/flavordb2/",
            retrieved_at=datetime(2024, 1, 15),
            parser_version="0.1.0",
        )

        mol = Tier1Molecule(
            molecule_id="pubchem:2244",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            _sources={"flavordb2": source_meta},
            pubchem_cid=AttributedValue(value=2244, sources=["flavordb2"]),
            smiles=AttributedValue(value="CC(=O)OC1=CC=CC=C1C(=O)O", sources=["pubchem"]),
            name=AttributedValue(value="Aspirin", sources=["flavordb2"]),
            flavor_descriptors=AttributedValue(value=["bitter", "sour"], sources=["flavordb2"]),
            molecular_weight=AttributedValue(value=180.16, unit="g/mol", sources=["pubchem"]),
        )

        assert mol.molecule_id == "pubchem:2244"
        assert mol.pubchem_cid.value == 2244
        assert mol.smiles.sources == ["pubchem"]
        assert "bitter" in mol.flavor_descriptors.value

    def test_tier1_sparse_fields(self):
        """Test that sparse schema allows missing fields."""
        mol = Tier1Molecule(
            molecule_id="cas:50-00-0",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            cas=AttributedValue(value="50-00-0", sources=["fenaroli"]),
            name=AttributedValue(value="Formaldehyde", sources=["fenaroli"]),
        )

        # Only populated fields
        assert mol.cas.value == "50-00-0"
        assert mol.name.value == "Formaldehyde"

        # Sparse fields are None
        assert mol.pubchem_cid is None
        assert mol.smiles is None
        assert mol.inchi is None
        assert mol.flavor_descriptors is None

    def test_tier1_extra_fields(self):
        """Test escape hatch for source-specific fields."""
        mol = Tier1Molecule(
            molecule_id="flavordb:123",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            extra={
                "fl_no": AttributedValue(value="02.001", sources=["vcf"]),
                "fema_no": AttributedValue(value="2001", sources=["fenaroli"]),
            },
        )

        assert mol.extra["fl_no"].value == "02.001"
        assert mol.extra["fema_no"].sources == ["fenaroli"]

    def test_tier1_multiple_sources(self):
        """Test molecule with data from multiple sources."""
        mol = Tier1Molecule(
            molecule_id="pubchem:123",
            _ingest_metadata=IngestMetadata(pipeline_version="0.1.0"),
            _sources={
                "flavordb2": SourceMetadata(
                    name="flavordb2",
                    version="2024.1",
                    retrieved_at=datetime.now(),
                    parser_version="0.1.0",
                ),
                "bitterdb": SourceMetadata(
                    name="bitterdb",
                    version="2024",
                    retrieved_at=datetime.now(),
                    parser_version="0.1.0",
                ),
            },
            smiles=AttributedValue(
                value="CCO",
                sources=["flavordb2", "bitterdb"],  # Both sources agree
            ),
        )

        assert len(mol.sources) == 2
        assert "flavordb2" in mol.sources
        assert "bitterdb" in mol.sources
        assert len(mol.smiles.sources) == 2

    def test_tier1_model_dump(self):
        """Test serialization of molecule."""
        mol = Tier1Molecule(
            molecule_id="test:1",
            _ingest_metadata=IngestMetadata(
                ingested_at=datetime(2024, 1, 1),
                pipeline_version="0.1.0",
            ),
            name=AttributedValue(value="Test", sources=["source1"]),
        )

        data = mol.model_dump()
        assert data["molecule_id"] == "test:1"
        assert data["name"]["value"] == "Test"
