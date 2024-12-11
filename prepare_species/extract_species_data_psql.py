import argparse
import importlib
import logging
import math
import os
from functools import partial
from multiprocessing import Pool
from typing import List, Optional, Set, Tuple

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

COLUMNS = [
    "id_no",
    "season",
    "elevation_lower",
    "elevation_upper",
    "full_habitat_code",
    "scientific_name",
    "family_name",
    "class_name",
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
    taxons.family_name
FROM
    assessments
    LEFT JOIN taxons ON taxons.id = assessments.taxon_id
    LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessment_id = assessments.id
    LEFT JOIN red_list_category_lookup ON red_list_category_lookup.id = assessments.red_list_category_id
WHERE
    assessments.latest = true
    AND taxons.class_name = %s
    AND red_list_category_lookup.code IN ('NT', 'VU', 'EN', 'CR')
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

def tidy_reproject_save(
    gdf: gpd.GeoDataFrame,
    output_directory_path: str,
    target_projection: Optional[str],
) -> None:
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = pyproj.CRS.from_string(target_projection) if target_projection else src_crs

    graw = gdf.loc[0].copy()
    grow = aoh_cleaning.tidy_data(graw)
    os.makedirs(output_directory_path, exist_ok=True)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")

def process_habitats(habitats_data: List) -> Set:
    if len(habitats_data) == 0:
        raise ValueError("No habitats found")
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

    return habitats

def process_geometries(geometries_data: List[Tuple[int,shapely.Geometry]]) -> shapely.Geometry:
    if len(geometries_data) == 0:
        raise ValueError("No geometries in DB")

    geometry = None
    for geometry_row in geometries_data:
        assert len(geometry_row) == 1
        row_geometry = geometry_row[0]
        if row_geometry is None:
            continue

        grange = shapely.normalize(shapely.from_wkb(row_geometry.to_ewkb()))
        if geometry is None:
            geometry = grange
        else:
            geometry = shapely.union(geometry, grange)

    if geometry is None:
        raise ValueError("None geometry data in DB")

    return geometry

def process_row(
    class_name: str,
    output_directory_path: str,
    target_projection: Optional[str],
    presence: Tuple[int],
    row: Tuple,
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    cursor = connection.cursor()

    id_no, assessment_id, possibly_extinct, possibly_extinct_in_the_wild, \
        elevation_lower, elevation_upper, scientific_name, family_name = row

    cursor.execute(HABITATS_STATEMENT, (assessment_id,))
    raw_habitats = cursor.fetchall()
    try:
        habitats = process_habitats(raw_habitats)
    except ValueError as exc:
        logging.info("Dropping %s: %s", id_no, str(exc))
        return

    # From Chess STAR report
    if possibly_extinct or possibly_extinct_in_the_wild:
        presence += (4,)

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    try:
        geometry = process_geometries(geometries_data)
    except ValueError as exc:
        logging.info("Dropping %s: %s", id_no, str(exc))
        return

    gdf = gpd.GeoDataFrame(
        [[
            id_no,
            "all",
            int(elevation_lower) if elevation_lower is not None else None,
            int(elevation_upper) if elevation_upper is not None else None,
            '|'.join(list(habitats)),
            scientific_name,
            family_name,
            class_name,
            geometry
          ]],
        columns=COLUMNS,
        crs=target_projection or 'epsg:4326'
    )
    tidy_reproject_save(gdf, output_directory_path, target_projection)

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
    overrides_path: str,
    output_directory_path: str,
    target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    cursor = connection.cursor()

    # For STAR-R we need historic data, but for STAR-T we just need current.
    # for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
    for era, presence in [("current", (1, 2))]:
        era_output_directory_path = os.path.join(output_directory_path, era)

        cursor.execute(MAIN_STATEMENT, (class_name,))
        # This can be quite big (tens of thousands), but in modern computer term is quite small
        # and I need to make a follow on DB query per result.
        results = cursor.fetchall()

        logger.info("Found %d species in class %s in scenarion %s", len(results), class_name, era)

        if overrides_path:
            results = apply_overrides(overrides_path, results)

        # The limiting amount here is how many concurrent connections the database can take
        with Pool(processes=20) as pool:
            pool.map(partial(process_row, class_name, era_output_directory_path, target_projection, presence), results)

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
        args.output_directory_path,
        args.target_projection
    )

if __name__ == "__main__":
    main()
