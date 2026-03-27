"""Tests for food flavor composition sources."""

from pathlib import Path

from flavor_pipeline.derived.food_composition.sources import (
    FoodAtlasFlavorFoodSource,
    FooDBFlavorFoodSource,
)


class TestFooDBFlavorFoodSource:
    """Tests for FooDBFlavorFoodSource."""

    def test_source_properties(self):
        """Test source metadata properties."""
        source = FooDBFlavorFoodSource()
        assert source.name == "foodb"
        assert source.version == "2020.04"
        assert "FooDB" in str(source.raw_data_dir)

    def test_validate_missing_files(self, tmp_path: Path):
        """Test validation when files are missing."""
        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        errors = source.validate()
        assert len(errors) > 0
        assert "Missing file" in errors[0]

    def test_validate_with_files(self, tmp_path: Path):
        """Test validation passes when files exist."""
        # Create the expected directory structure
        foodb_dir = tmp_path / "FooDB" / "foodb_2020_04_07_csv"
        foodb_dir.mkdir(parents=True)

        # Create required files
        (foodb_dir / "Food.csv").write_text("id,name\n1,Apple\n")
        (foodb_dir / "Content.csv").write_text("food_id,source_id,source_type\n")
        (foodb_dir / "Compound.csv").write_text("id,name\n")
        (foodb_dir / "CompoundsFlavor.csv").write_text("compound_id,flavor_id\n")

        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        errors = source.validate()
        assert errors == []

    def test_parse_empty_dir(self, tmp_path: Path):
        """Test parsing when data directory doesn't exist."""
        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()
        assert records == []

    def test_parse_with_sample_data(self, tmp_path: Path):
        """Test parsing with sample FooDB data."""
        foodb_dir = tmp_path / "FooDB" / "foodb_2020_04_07_csv"
        foodb_dir.mkdir(parents=True)

        # Create Food.csv
        (foodb_dir / "Food.csv").write_text(
            "id,public_id,name,name_scientific,food_group\n"
            "1,FOOD00001,Apple,Malus domestica,Fruits\n"
            "2,FOOD00002,Banana,Musa acuminata,Fruits\n"
        )

        # Create Compound.csv (with column shift simulation)
        # Due to FooDB column shift: description=CAS, moldb_smiles=InChIKey
        (foodb_dir / "Compound.csv").write_text(
            "id,public_id,name,description,moldb_smiles\n"
            "101,FDB000101,Limonene,138-86-3,XMGQYMWWDOXHJM-UHFFFAOYSA-N\n"
            "102,FDB000102,Ethanol,64-17-5,LFQSCWFLJHTTHZ-UHFFFAOYSA-N\n"
        )

        # Create Flavor.csv
        (foodb_dir / "Flavor.csv").write_text(
            "id,name\n"
            "1,citrus\n"
            "2,orange\n"
        )

        # Create CompoundsFlavor.csv - marks which compounds are flavor compounds
        (foodb_dir / "CompoundsFlavor.csv").write_text(
            "compound_id,flavor_id\n"
            "101,1\n"
            "101,2\n"
        )

        # Create Content.csv - food-compound associations
        (foodb_dir / "Content.csv").write_text(
            "food_id,source_id,source_type,orig_content,orig_min,orig_max,orig_unit,preparation\n"
            "1,101,Compound,15.5,10.0,25.0,mg/100g,whole\n"
            "1,102,Compound,5.0,,,ppm,\n"  # Non-flavor compound
            "2,101,Compound,8.2,,,mg/100g,peel\n"
        )

        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()

        # Should only have 2 records (flavor compound 101 in food 1 and 2)
        # Compound 102 is not in CompoundsFlavor, so it should be excluded
        assert len(records) == 2

        # Check first record (Apple with Limonene)
        apple_record = [r for r in records if r.food_name == "Apple"][0]
        assert apple_record.scientific_name == "Malus domestica"
        assert apple_record.food_category == "Fruits"
        assert apple_record.molecule_name == "Limonene"
        assert apple_record.cas == "138-86-3"
        assert apple_record.inchikey == "XMGQYMWWDOXHJM-UHFFFAOYSA-N"
        assert apple_record.molecule_id == "inchikey:XMGQYMWWDOXHJM-UHFFFAOYSA-N"
        assert apple_record.concentration == 15.5
        assert apple_record.concentration_min == 10.0
        assert apple_record.concentration_max == 25.0
        assert apple_record.concentration_unit == "mg/100g"
        assert "citrus" in apple_record.flavor_descriptors
        assert "orange" in apple_record.flavor_descriptors
        assert apple_record.source == "foodb"
        assert apple_record.food_part == "whole"

        # Check banana record
        banana_record = [r for r in records if r.food_name == "Banana"][0]
        assert banana_record.food_part == "peel"
        assert banana_record.concentration == 8.2


class TestFoodAtlasFlavorFoodSource:
    """Tests for FoodAtlasFlavorFoodSource."""

    def test_source_properties(self):
        """Test source metadata properties."""
        source = FoodAtlasFlavorFoodSource()
        assert source.name == "foodatlas"
        assert source.version == "3.2.0"
        assert "Foodatlas" in str(source.raw_data_dir)

    def test_validate_missing_files(self, tmp_path: Path):
        """Test validation when files are missing."""
        source = FoodAtlasFlavorFoodSource(raw_data_base=tmp_path)
        errors = source.validate()
        assert len(errors) > 0
        assert "Missing file" in errors[0]

    def test_validate_with_files(self, tmp_path: Path):
        """Test validation passes when files exist."""
        # Create the expected directory structure
        atlas_dir = tmp_path / "Foodatlas" / "v3.2_20250211"
        atlas_dir.mkdir(parents=True)

        # Create required files
        (atlas_dir / "entities.tsv").write_text("foodatlas_id\tentity_type\n")
        (atlas_dir / "triplets.tsv").write_text("head_id\ttail_id\n")
        (atlas_dir / "metadata_flavor.tsv").write_text("_chemical_name\t_flavor_name\n")

        source = FoodAtlasFlavorFoodSource(raw_data_base=tmp_path)
        errors = source.validate()
        assert errors == []

    def test_parse_empty_dir(self, tmp_path: Path):
        """Test parsing when data directory doesn't exist."""
        source = FoodAtlasFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()
        assert records == []

    def test_parse_with_sample_data(self, tmp_path: Path):
        """Test parsing with sample FoodAtlas data."""
        atlas_dir = tmp_path / "Foodatlas" / "v3.2_20250211"
        atlas_dir.mkdir(parents=True)

        # Create entities.tsv with foods and chemicals
        (atlas_dir / "entities.tsv").write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name\texternal_ids\n"
            "e1\tfood\tApple\tMalus domestica\t{}\n"
            "e2\tfood\tBanana\tMusa acuminata\t{}\n"
            "e100\tchemical\tLimonene\t\t{'pubchem_compound': [22311]}\n"
            "e101\tchemical\tEthanol\t\t{'pubchem_compound': [702]}\n"
        )

        # Create metadata_flavor.tsv - identifies flavor chemicals by PubChem ID
        (atlas_dir / "metadata_flavor.tsv").write_text(
            "_chemical_name\t_flavor_name\n"
            "PUBCHEM_COMPOUND:22311\tcitrus\n"
            "PUBCHEM_COMPOUND:22311\torange\n"
        )

        # Create triplets.tsv - food contains chemical relationships
        (atlas_dir / "triplets.tsv").write_text(
            "head_id\ttail_id\trelationship_id\tmetadata_ids\n"
            "e1\te100\tr1\t['m1']\n"
            "e1\te101\tr1\t[]\n"  # Non-flavor chemical
            "e2\te100\tr1\t['m2']\n"
        )

        # Create metadata_contains.tsv - concentration data
        (atlas_dir / "metadata_contains.tsv").write_text(
            "foodatlas_id\tconc_value\tconc_unit\n"
            "m1\t25.5\tmg/100g\n"
            "m2\t12.0\tppm\n"
        )

        source = FoodAtlasFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()

        # Should have 2 records (flavor chemical e100 in food e1 and e2)
        # e101 is not in metadata_flavor, so it should be excluded
        assert len(records) == 2

        # Check apple record
        apple_record = [r for r in records if r.food_name == "Apple"][0]
        assert apple_record.scientific_name == "Malus domestica"
        assert apple_record.molecule_name == "Limonene"
        assert apple_record.pubchem_id == 22311
        assert apple_record.molecule_id == "pubchem:22311"
        assert apple_record.concentration == 25.5
        assert apple_record.concentration_unit == "mg/100g"
        assert "citrus" in apple_record.flavor_descriptors
        assert "orange" in apple_record.flavor_descriptors
        assert apple_record.source == "foodatlas"

        # Check banana record
        banana_record = [r for r in records if r.food_name == "Banana"][0]
        assert banana_record.concentration == 12.0
        assert banana_record.concentration_unit == "ppm"


class TestMoleculeIdPriority:
    """Tests for molecule_id generation priority."""

    def test_foodb_inchikey_priority(self, tmp_path: Path):
        """Test that InChIKey takes priority over CAS for FooDB."""
        foodb_dir = tmp_path / "FooDB" / "foodb_2020_04_07_csv"
        foodb_dir.mkdir(parents=True)

        # All fields present - InChIKey should be used
        (foodb_dir / "Food.csv").write_text("id,public_id,name\n1,FOOD00001,Apple\n")
        (foodb_dir / "Compound.csv").write_text(
            "id,public_id,name,description,moldb_smiles,pubchem_compound_id\n"
            "101,FDB000101,Test,138-86-3,XMGQYMWWDOXHJM-UHFFFAOYSA-N,22311\n"
        )
        (foodb_dir / "Flavor.csv").write_text("id,name\n1,test\n")
        (foodb_dir / "CompoundsFlavor.csv").write_text("compound_id,flavor_id\n101,1\n")
        (foodb_dir / "Content.csv").write_text(
            "food_id,source_id,source_type\n1,101,Compound\n"
        )

        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()

        assert len(records) == 1
        assert records[0].molecule_id == "inchikey:XMGQYMWWDOXHJM-UHFFFAOYSA-N"
        assert records[0].cas == "138-86-3"
        assert records[0].inchikey == "XMGQYMWWDOXHJM-UHFFFAOYSA-N"

    def test_foodb_cas_fallback(self, tmp_path: Path):
        """Test that CAS is used when InChIKey is missing."""
        foodb_dir = tmp_path / "FooDB" / "foodb_2020_04_07_csv"
        foodb_dir.mkdir(parents=True)

        # No InChIKey - CAS should be used
        (foodb_dir / "Food.csv").write_text("id,public_id,name\n1,FOOD00001,Apple\n")
        (foodb_dir / "Compound.csv").write_text(
            "id,public_id,name,description,moldb_smiles\n"
            "101,FDB000101,Test,138-86-3,\n"  # Empty InChIKey
        )
        (foodb_dir / "Flavor.csv").write_text("id,name\n1,test\n")
        (foodb_dir / "CompoundsFlavor.csv").write_text("compound_id,flavor_id\n101,1\n")
        (foodb_dir / "Content.csv").write_text(
            "food_id,source_id,source_type\n1,101,Compound\n"
        )

        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()

        assert len(records) == 1
        assert records[0].molecule_id == "cas:138-86-3"
        assert records[0].cas == "138-86-3"
        assert records[0].inchikey is None

    def test_foodb_source_id_fallback(self, tmp_path: Path):
        """Test that FooDB ID is used when no other IDs available."""
        foodb_dir = tmp_path / "FooDB" / "foodb_2020_04_07_csv"
        foodb_dir.mkdir(parents=True)

        # No InChIKey, no valid CAS - FooDB ID should be used
        (foodb_dir / "Food.csv").write_text("id,public_id,name\n1,FOOD00001,Apple\n")
        (foodb_dir / "Compound.csv").write_text(
            "id,public_id,name,description,moldb_smiles\n"
            "101,FDB000101,Test,invalid_cas,\n"
        )
        (foodb_dir / "Flavor.csv").write_text("id,name\n1,test\n")
        (foodb_dir / "CompoundsFlavor.csv").write_text("compound_id,flavor_id\n101,1\n")
        (foodb_dir / "Content.csv").write_text(
            "food_id,source_id,source_type\n1,101,Compound\n"
        )

        source = FooDBFlavorFoodSource(raw_data_base=tmp_path)
        records = source.parse()

        assert len(records) == 1
        assert records[0].molecule_id == "foodb:FDB000101"
        assert records[0].cas is None  # Invalid CAS should be None
        assert records[0].inchikey is None
