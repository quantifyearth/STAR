import pytest
import postgis
import shapely

from prepare_species.extract_species_data_psql import process_habitats, process_geometries, \
    process_threats, SpeciesReport

def test_empty_report() -> None:
    report = SpeciesReport(1, 2, "name")

    assert report.id_no == 1
    assert report.assessment_id == 2
    assert report.scientific_name == "name"
    assert not report.possibly_extinct
    assert not report.has_habitats
    assert not report.keeps_habitats
    assert not report.has_threats
    assert not report.has_geometries
    assert not report.keeps_geometries
    assert not report.filename

    row = report.as_row()
    assert row[:3] == [1, 2, "name"]
    assert not all(row[3:])

def test_simple_example():
    habitat_data = [("4.1|4.2",)]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    assert res == set(["4.1", "4.2"])
    assert report.has_habitats
    assert report.keeps_habitats

def test_no_habitats_in_db():
    habitat_data = []
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)
    assert res == set(["18"])
    assert not report.has_habitats
    assert report.keeps_habitats

def test_too_many_habitats_in_db():
    habitat_data = [("4.1|4.2",), ("1.2",)]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert not report.keeps_habitats

def test_empty_habitat_list():
    habitat_data = [("|",)]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert not report.keeps_habitats

def test_empty_geometry_list():
    geoemetries_data = []
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_geometries(geoemetries_data, report)
    assert not report.has_geometries
    assert not report.keeps_geometries

def test_simple_resident_species_geometry_filter_point():
    geoemetries_data = [
        (postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000"),),
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_geometries(geoemetries_data, report)
    assert report.has_geometries
    assert not report.keeps_geometries

def test_simple_resident_species_geometry_filter_polygon():
    geoemetries_data = [
        (postgis.Geometry.from_ewkb("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F9A9999999999B93F000000000000F03F00000000000000000000000000000000"),), # pylint: disable=C0301
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_geometries(geoemetries_data, report)

    assert res == shapely.Polygon([(0.0, 0.0), (0.1, 1.0), (1.0, 1.0), (0.0, 0.0)])
    assert report.has_geometries
    assert report.keeps_geometries

def test_simple_migratory_species_geometry_filter():
    geoemetries_data = [
        (postgis.Geometry.from_ewkb("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F9A9999999999B93F000000000000F03F00000000000000000000000000000000"),), # pylint: disable=C0301
        (postgis.Geometry.from_ewkb("0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F9A9999999999B93F000000000000F03F00000000000000000000000000000000"),), # pylint: disable=C0301
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_geometries(geoemetries_data, report)
    assert res == shapely.Polygon([(0.1, 1.0), (1.0, 1.0), (0.0, 0.0), (0.1, 1.0)])
    assert report.has_geometries
    assert report.keeps_geometries

def test_empty_threat_list():
    threats_data = []
    report = SpeciesReport(1, 2, "name")
    res = process_threats(threats_data, report)
    assert not res
    assert not report.has_threats

def test_no_serious_threats():
    threats_data = [
        ("Minority (<50%)", "No decline"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_threats(threats_data, report)
    assert not res
    assert not report.has_threats

def test_serious_threats():
    threats_data = [
        ("Whole (>90%)", "Very rapid declines"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_threats(threats_data, report)
    assert res
    assert report.has_threats

def test_mixed_threats():
    threats_data = [
        ("Whole (>90%)", "Very rapid declines"),
        ("Minority (<50%)", "No decline"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_threats(threats_data, report)
    assert res
    assert report.has_threats
