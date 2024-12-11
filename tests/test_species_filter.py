import pytest
import postgis
import shapely

from prepare_species.extract_species_data_psql import process_habitats, process_geometries

def test_simple_example():
    habitat_data = [("4.1|4.2",)]
    res = process_habitats(habitat_data)

    # Just resident
    assert res == set(["4.1", "4.2"])

def test_no_habitats_in_db():
    habitat_data = []
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_too_many_habitats_in_db():
    habitat_data = [("4.1|4.2",), ("1.2",)]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_empty_habitat_list():
    habitat_data = [("|",)]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_empty_geometry_list():
    geoemetries_data = []
    with pytest.raises(ValueError):
        _ = process_geometries(geoemetries_data)

def test_simple_resident_species_geometry_filter():
    geoemetries_data = [
        (postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000"),),
    ]
    res = process_geometries(geoemetries_data)

    assert res == shapely.Point(2, 4)

def test_simple_migratory_species_geometry_filter():
    geoemetries_data = [
        (postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000"),),
        (postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000"),),
    ]
    res = process_geometries(geoemetries_data)
    assert res == shapely.Point(2, 4)
