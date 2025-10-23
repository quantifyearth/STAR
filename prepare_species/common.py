import os
from pathlib import Path
from typing import Any, Optional

import aoh
import geopandas as gpd
import pyproj
import shapely

# To match the FABDEM elevation map we use
# different range min/max/separation
ELEVATION_MAX = 8580
ELEVATION_MIN = -427
ELEVATION_SPREAD = 12

COLUMNS = [
    "id_no",
    "assessment_id",
    "assessment_year",
    "season",
    "systems",
    "elevation_lower",
    "elevation_upper",
    "full_habitat_code",
    "scientific_name",
    "family_name",
    "class_name",
    "threats",
    "category",
    "category_weight",
    "geometry"
]

# From Muir et al: For each species, a global STAR threat abatement (START) score
# is defined. This varies from zero for species of Least Concern to 100
# for Near Threatened, 200 for Vulnerable, 300 for Endangered and
# 400 for Critically Endangered species (using established weighting
# ratios7,8)
CATEGORY_WEIGHTS = {
    'NT': 100,
    'VU': 200,
    'EN': 300,
    'CR': 400,
}

# Mapping from API scope values to STAR scope indices
SCOPES = [
    "whole (>90%)",
    "majority (50-90%)",
    "minority (<50%)"
]
DEFAULT_SCOPE = "majority (50-90%)"

# Mapping from API severity values to STAR severity indices
SEVERITIES = [
    "very rapid declines",
    "rapid declines",
    "slow, significant declines",
    "negligible declines",
    "no decline",
    "causing/could cause fluctuations"
]
DEFAULT_SEVERITY = "slow, significant declines"

# Taken from Muir et al 2021 Supplementary Table 2, indexed by SCOPE and then SEVERITY
THREAT_WEIGHTING_TABLE = [
    [63, 24, 10, 1, 0, 10],
    [52, 18,  9, 0, 0,  9],
    [24,  7,  5, 0, 0,  5],
]

class SpeciesReport:

    REPORT_COLUMNS = [
        "id_no",
        "assessment_id",
        "scientific_name",
        "has_api_data",
        "possibly_extinct",
        "has_systems",
        "not_terrestrial_system",
        "has_threats",
        "has_habitats",
        "keeps_habitats",
        "has_geometries",
        "keeps_geometries",
        "filename",
    ]

    def __init__(self, id_no, assessment_id, scientific_name):
        self.info = {k: False for k in self.REPORT_COLUMNS}
        self.id_no = id_no
        self.assessment_id = assessment_id
        self.scientific_name = scientific_name

    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.REPORT_COLUMNS:
            self.info[name] = value
        super().__setattr__(name, value)

    def __getattr__(self, name: str) -> Any:
        if name in self.REPORT_COLUMNS:
            return self.info[name]
        return None

    def as_row(self) -> list:
        return [self.info[k] for k in self.REPORT_COLUMNS]

def process_geometries(
    geometries_data: list[shapely.Geometry],
    report: SpeciesReport,
) -> shapely.Geometry:
    if len(geometries_data) == 0:
        raise ValueError("No geometries in DB")
    report.has_geometries = True

    geometry = None
    for geometry in geometries_data:
        grange = shapely.normalize(geometry)
        if grange.area == 0.0:
            continue

        if geometry is None:
            geometry = grange
        else:
            geometry = shapely.union(geometry, grange)

    if geometry is None:
        raise ValueError("None geometry data in DB")
    report.keeps_geometries = True

    return geometry


def process_systems(
    systems_data: list[tuple],
    report: SpeciesReport,
) -> list:
    if len(systems_data) == 0:
        raise ValueError("No systems found")
    if len(systems_data) > 1:
        raise ValueError("More than one systems aggregation found")
    systems = systems_data[0][0]
    if systems is None:
        raise ValueError("no systems info")
    report.has_systems = True

    if "Terrestrial" not in systems:
        raise ValueError("No Terrestrial in systems")
    report.not_terrestrial_system = True

    return systems

def process_threats(
    threat_data: list[tuple[int, str, str]],
    report: SpeciesReport,
) -> list[tuple[int, int]]:
    cleaned_threats = []
    for code, scope, severity in threat_data:
        if scope is None or scope.lower() == "unknown":
            scope = DEFAULT_SCOPE
        if severity is None or severity.lower() == "unknown":
            severity = DEFAULT_SEVERITY
        scope_index = SCOPES.index(scope.lower())
        severity_index = SEVERITIES.index(severity.lower())
        score = THREAT_WEIGHTING_TABLE[scope_index][severity_index]
        if score > 0:
            cleaned_threats.append((code, score))
    report.has_threats = len(cleaned_threats) > 0
    return cleaned_threats

def process_habitats(
    habitats_data: list[list[str]],
    report: SpeciesReport,
) -> set:
    if len(habitats_data) == 0:
        # Promote to "Unknown"
        habitats_data = [["18"]]
    else:
        report.has_habitats = True
    if len(habitats_data) > 1:
        raise ValueError("Expected only one habitat row")

    habitats = set()
    for habitat_values_row in habitats_data:
        assert len(habitat_values_row) == 1
        habitat_values = habitat_values_row[0]

        if habitat_values is None:
            habitat_values = "18"
        habitat_set = {x for x in habitat_values.split('|') if x}
        habitats |= habitat_set

    if len(habitats) == 0:
        raise ValueError("No filtered habitats")
    report.keeps_habitats = True

    return habitats


def tidy_reproject_save(
    gdf: gpd.GeoDataFrame,
    report: SpeciesReport,
    output_directory_path: Path,
    target_projection: Optional[str],
) -> None:
    """Tidy the data, reproject it, and save to GeoJSON."""
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = pyproj.CRS.from_string(target_projection) if target_projection else src_crs

    graw = gdf.loc[0].copy()
    grow = aoh.tidy_data(
        graw,
        elevation_max=ELEVATION_MAX,
        elevation_min=ELEVATION_MIN,
        elevation_seperation=ELEVATION_SPREAD,
    )
    os.makedirs(output_directory_path, exist_ok=True)
    output_path = output_directory_path / f"{grow.id_no}.geojson"
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")
    report.filename = output_path
