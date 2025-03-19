import argparse
import importlib
import logging
import math
import os
from functools import partial
from multiprocessing import Pool
from typing import Any, List, Optional, Set, Tuple

# import pyshark # pylint: disable=W0611
import geopandas as gpd
import pandas as pd
import pyproj
import psycopg2
import shapely
from postgis.psycopg import register

aoh_cleaning = importlib.import_module("aoh-calculator.cleaning")

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

# To match the FABDEM elevation map we use
# different range min/max/seperation
ELEVATION_MAX = 8580
ELEVATION_MIN = -427
ELEVATION_SPREAD = 12

COLUMNS = [
    "id_no",
    "assessment_id",
    "season",
    "systems",
    "elevation_lower",
    "elevation_upper",
    "full_habitat_code",
    "scientific_name",
    "family_name",
    "class_name",
    "category",
    "geometry"
]

MAIN_STATEMENT = """
SELECT
    assessments.sis_taxon_id as id_no,
    assessments.id as assessment_id,
    assessments.possibly_extinct,
    assessments.possibly_extinct_in_the_wild,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationLower.limit')::numeric AS elevation_lower,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationUpper.limit')::numeric AS elevation_upper,
    taxons.scientific_name,
    taxons.family_name,
    red_list_category_lookup.code
FROM
    assessments
    LEFT JOIN assessment_scopes ON assessment_scopes.assessment_id = assessments.id
    LEFT JOIN taxons ON taxons.id = assessments.taxon_id
    LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessment_id = assessments.id
    LEFT JOIN red_list_category_lookup ON red_list_category_lookup.id = assessments.red_list_category_id
WHERE
    assessments.latest = true
    AND assessments.sis_taxon_id NOT IN %s
    AND assessment_scopes.scope_lookup_id = 15 -- global assessments only
    AND taxons.class_name = %s
    AND taxons.infra_type is NULL -- no subspecies
    AND taxons.metadata->>'taxon_level' = 'Species'
    AND red_list_category_lookup.code IN ('NT', 'VU', 'EN', 'CR')
"""

SYSTEMS_STATEMENT = """
SELECT
    STRING_AGG(system_lookup.description->>'en', '|') AS systems
FROM
    assessments
    LEFT JOIN assessment_systems ON assessment_systems.assessment_id = assessments.id
    LEFT JOIN system_lookup ON assessment_systems.system_lookup_id = system_lookup.id
WHERE
    assessments.id = %s
GROUP BY
    assessments.id
"""

THREATS_STATEMENT = """
SELECT
    supplementary_fields->>'scope' AS scope,
    supplementary_fields->>'severity' AS severity
FROM
    assessment_threats
WHERE
    assessment_id = %s
    AND (supplementary_fields->>'timing' is NULL OR supplementary_fields->>'timing' <> 'Past, Unlikely to Return')
"""

HABITATS_STATEMENT = """
SELECT
    STRING_AGG(habitat_lookup.code, '|') AS full_habitat_code
FROM
    assessments
    LEFT JOIN assessment_habitats ON assessment_habitats.assessment_id = assessments.id
    LEFT JOIN habitat_lookup on habitat_lookup.id = assessment_habitats.habitat_id
WHERE
    assessments.id = %s
"""

GEOMETRY_STATEMENT = """
SELECT
    ST_UNION(assessment_ranges.geom::geometry) AS geometry
FROM
    assessments
    LEFT JOIN assessment_ranges On assessment_ranges.assessment_id = assessments.id
WHERE
    assessments.id = %s
    AND assessment_ranges.presence IN %s
    AND assessment_ranges.origin IN (1, 2, 6)
"""

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_CONFIG = (
	f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

class SpeciesReport:

    REPORT_COLUMNS = [
        "id_no",
        "assessment_id",
        "scientific_name",
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

    def as_row(self) -> List:
        return [self.info[k] for k in self.REPORT_COLUMNS]

def tidy_reproject_save(
    gdf: gpd.GeoDataFrame,
    report: SpeciesReport,
    output_directory_path: str,
    target_projection: Optional[str],
) -> None:
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = pyproj.CRS.from_string(target_projection) if target_projection else src_crs

    graw = gdf.loc[0].copy()
    grow = aoh_cleaning.tidy_data(
        graw,
        elevation_max=ELEVATION_MAX,
        elevation_min=ELEVATION_MIN,
        elevation_seperation=ELEVATION_SPREAD,
    )
    os.makedirs(output_directory_path, exist_ok=True)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")
    report.filename = output_path

def process_systems(
    systems_data: List[Tuple],
    report: SpeciesReport,
) -> None:
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

SCOPES = [
    "whole (>90%)",
    "majority (50-90%)",
    "minority (<50%)"
]
DEFAULT_SCOPE = "majority (50-90%)"
SEVERITIES = [
    "very rapid declines",
    "rapid declines",
    "slow, significant declines",
    "negligible declines",
    "no decline",
    "causing/could cause fluctuations"
]
DEFAULT_SEVERITY = "slow, significant declines"

# Taken from Muir et al 2021, indexed by SCOPE and then SEVERITY
THREAT_WEIGHTING_TABLE = [
    [63, 24, 10, 1, 0, 10],
    [52, 18,  9, 0, 0,  9],
    [24,  7,  5, 0, 0,  5],
]

def process_threats(
    threat_data: List,
    report: SpeciesReport,
) -> bool:
    total = 0
    for scope, severity in threat_data:
        if scope is None or scope.lower() == "unknown":
            scope = DEFAULT_SCOPE
        if severity is None or severity.lower() == "unknown":
            severity = DEFAULT_SEVERITY
        scope_index = SCOPES.index(scope.lower())
        severity_index = SEVERITIES.index(severity.lower())
        score = THREAT_WEIGHTING_TABLE[scope_index][severity_index]
        total += score
    report.has_threats = total != 0
    return total != 0

def process_habitats(
    habitats_data: List,
    report: SpeciesReport,
) -> Set:
    if len(habitats_data) == 0:
        raise ValueError("No habitats found")
    report.has_habitats = True
    if len(habitats_data) > 1:
        raise ValueError("Expected only one habitat row")

    habitats = set()
    for habitat_values_row in habitats_data:
        assert len(habitat_values_row) == 1
        habitat_values = habitat_values_row[0]

        if habitat_values is None:
            continue
        habitat_set = {x for x in habitat_values.split('|') if x}
        habitats |= habitat_set

    if len(habitats) == 0:
        raise ValueError("No filtered habitats")
    report.keeps_habitats = True

    return habitats

def process_geometries(
    geometries_data: List[Tuple[int,shapely.Geometry]],
    report: SpeciesReport,
) -> shapely.Geometry:
    if len(geometries_data) == 0:
        raise ValueError("No geometries in DB")
    report.has_geometries = True

    geometry = None
    for geometry_row in geometries_data:
        assert len(geometry_row) == 1
        row_geometry = geometry_row[0]
        if row_geometry is None:
            continue

        grange = shapely.normalize(shapely.from_wkb(row_geometry.to_ewkb()))
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

def process_row(
    class_name: str,
    output_directory_path: str,
    target_projection: Optional[str],
    presence: Tuple[int],
    row: Tuple,
) -> Tuple:

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    cursor = connection.cursor()

    id_no, assessment_id, possibly_extinct, possibly_extinct_in_the_wild, \
        elevation_lower, elevation_upper, scientific_name, family_name, category = row

    report = SpeciesReport(id_no, assessment_id, scientific_name)

    # From Chess STAR report
    if possibly_extinct or possibly_extinct_in_the_wild:
        presence += (4,)
        report.possibly_extinct = True # pylint: disable=W0201


    cursor.execute(SYSTEMS_STATEMENT, (assessment_id,))
    systems_data = cursor.fetchall()
    try:
        systems = process_systems(systems_data, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, str(exc))
        return report

    cursor.execute(THREATS_STATEMENT, (assessment_id,))
    raw_threats = cursor.fetchall()
    threatened = process_threats(raw_threats, report)
    if not threatened:
        return report

    cursor.execute(HABITATS_STATEMENT, (assessment_id,))
    raw_habitats = cursor.fetchall()
    try:
        habitats = process_habitats(raw_habitats, report)
    except ValueError as _exc:
        return report

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    try:
        geometry = process_geometries(geometries_data, report)
    except ValueError as _exc:
        return report

    gdf = gpd.GeoDataFrame(
        [[
            id_no,
            assessment_id,
            "all",
            systems,
            int(elevation_lower) if elevation_lower is not None else None,
            int(elevation_upper) if elevation_upper is not None else None,
            '|'.join(list(habitats)),
            scientific_name,
            family_name,
            class_name,
            category,
            geometry
          ]],
        columns=COLUMNS,
        crs=target_projection or 'epsg:4326'
    )
    tidy_reproject_save(gdf, report, output_directory_path, target_projection)
    return report

def apply_overrides(
    overrides_path: str,
    results,
):
    overrides = pd.read_csv(overrides_path, encoding="latin1")

    updated = []
    for row in results:
        updated_row = list(row)
        id_no = updated_row[0]

        override = overrides[overrides["SIS ID"] == id_no]
        if len(override) != 0:
            assert len(override) == 1
            occasional_lower = override.iloc[0]["Occasional lower elevation"]
            occasional_upper = override.iloc[0]["Occasional upper elevation"]

            if not math.isnan(occasional_lower):
                updated_row[4] = occasional_lower
            if not math.isnan(occasional_upper):
                updated_row[5] = occasional_upper

        updated.append(tuple(updated_row))

    return updated

def extract_data_per_species(
    class_name: str,
    overrides_path: Optional[str],
    excludes_path: Optional[str],
    output_directory_path: str,
    target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    cursor = connection.cursor()

    excludes = []
    if excludes_path is not None:
        try:
            df = pd.read_csv(excludes_path)
            excludes = tuple([int(x) for x in df.id_no.unique()]) # pylint: disable=R1728
            logger.info("Excluding %d species", len(excludes))
        except FileNotFoundError:
            pass

    # For STAR-R we need historic data, but for STAR-T we just need current.
    # for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
    for era, presence in [("current", (1, 2))]:
        era_output_directory_path = os.path.join(output_directory_path, era)

        cursor.execute(MAIN_STATEMENT, (excludes, class_name,))
        # This can be quite big (tens of thousands), but in modern computer term is quite small
        # and I need to make a follow on DB query per result.
        results = cursor.fetchall()

        logger.info("Found %d species in class %s in scenarion %s", len(results), class_name, era)

        if overrides_path:
            results = apply_overrides(overrides_path, results)

        # The limiting amount here is how many concurrent connections the database can take
        with Pool(processes=20) as pool:
            reports = pool.map(
                partial(process_row, class_name, era_output_directory_path, target_projection, presence),
                results
            )
        # reports = [
        #     process_row(class_name,  era_output_directory_path, target_projection, presence, x)
        #     for x in results[:10]
        # ]

        reports_df = pd.DataFrame(
            [x.as_row() for x in reports],
            columns=SpeciesReport.REPORT_COLUMNS
        ).sort_values('id_no')
        reports_df.to_csv(os.path.join(era_output_directory_path, "report.csv"), index=False)

def main() -> None:
    parser = argparse.ArgumentParser(description="Process agregate species data to per-species-file.")
    parser.add_argument(
        '--class',
        type=str,
        help="Species class name",
        required=True,
        dest="classname",
    )
    parser.add_argument(
        '--overrides',
        type=str,
        help="CSV of overrides",
        required=False,
        dest="overrides",
    )
    parser.add_argument(
        '--excludes',
        type=str,
        help="CSV of taxon IDs to not include",
        required=False,
        dest="excludes"
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Directory where per species Geojson is stored',
        required=True,
        dest='output_directory_path',
    )
    parser.add_argument(
        '--projection',
        type=str,
        help="Target projection",
        required=False,
        dest="target_projection",
        default="ESRI:54017"
    )
    args = parser.parse_args()

    extract_data_per_species(
        args.classname,
        args.overrides,
        args.excludes,
        args.output_directory_path,
        args.target_projection
    )

if __name__ == "__main__":
    main()
