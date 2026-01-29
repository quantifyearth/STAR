
import argparse
import json
import logging
import math
import os
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import psycopg2
import shapely
from postgis.psycopg import register
from snakemake_argparse_bridge import snakemake_compatible

from common import (
    CATEGORY_WEIGHTS,
    COLUMNS,
    process_habitats,
    process_threats,
    process_systems,
    process_geometries,
    tidy_reproject_save,
    SpeciesReport,
)

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

# Note that this query returns more species that would be accepted into STAR based on
# threat category, but for model checking we want all AOHs for a given taxa, so the
# pipeline must process them all, and then just include the correct set when processing
# STAR
MAIN_STATEMENT = """
SELECT
    assessments.sis_taxon_id as id_no,
    assessments.id as assessment_id,
    DATE_PART('year', assessments.assessment_date) as assessment_year,
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
    AND assessment_scopes.scope_lookup_id = 15 -- global assessments only
    AND taxons.class_name = %s
    AND taxons.infra_type is NULL -- no subspecies
    AND taxons.metadata->>'taxon_level' = 'Species'
    AND red_list_category_lookup.code IN ('DD', 'LC', 'NT', 'VU', 'EN', 'CR')
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
    threat_lookup.code,
    assessment_threats.supplementary_fields->>'scope' AS scope,
    assessment_threats.supplementary_fields->>'severity' AS severity
FROM
    assessment_threats
    LEFT JOIN threat_lookup ON assessment_threats.threat_id = threat_lookup.id
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

def process_row(
    class_name: str,
    output_directory_path: Path,
    target_projection: Optional[str],
    presence: tuple[int, ...],
    row: tuple,
) -> SpeciesReport:

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    cursor = connection.cursor()

    id_no, assessment_id, assessment_year, possibly_extinct, possibly_extinct_in_the_wild, \
        elevation_lower, elevation_upper, scientific_name, family_name, category = row

    report = SpeciesReport(id_no, assessment_id, scientific_name)
    report.has_api_data = True

    # From Chess STAR report
    if possibly_extinct or possibly_extinct_in_the_wild:
        presence += (4,)
        report.possibly_extinct = True # pylint: disable=W0201

    include_in_star = category in ('NT', 'VU', 'EN', 'CR')
    report.has_category = include_in_star

    # First checks are the ones that rule out being able to make an AOH at all
    cursor.execute(HABITATS_STATEMENT, (assessment_id,))
    raw_habitats = cursor.fetchall()
    try:
        habitats = process_habitats(raw_habitats, report)
    except ValueError as _exc:
        return report

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    cleaned_geometries = [
        shapely.from_wkb(row_geometry[0].to_ewkb())
        for row_geometry in geometries_data if row_geometry[0] is not None
    ]
    try:
        geometry = process_geometries(cleaned_geometries, report)
    except ValueError as _exc:
        return report

    # Second checks are whether it is good for STAR, so from hereon we should
    # output a GeoJSON regardless as we should make an AOH for validation with this
    cursor.execute(SYSTEMS_STATEMENT, (assessment_id,))
    systems_data = cursor.fetchall()
    try:
        systems = process_systems(systems_data, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, str(exc))
        include_in_star = False
        try:
            systems = systems_data[0][0]
        except IndexError:
            systems = []

    cursor.execute(THREATS_STATEMENT, (assessment_id,))
    raw_threats = cursor.fetchall()
    threats = process_threats(raw_threats, report)
    if len(threats) == 0:
        include_in_star = False

    report.in_star = include_in_star

    try:
        category_weight = CATEGORY_WEIGHTS[category]
    except KeyError:
        assert include_in_star is False
        category_weight = 0

    # This is a fix as per the method to include the missing islands layer:
    habitats_list = list(habitats) + ["islands"]

    gdf = gpd.GeoDataFrame(
        [[
            id_no,
            assessment_id,
            int(assessment_year),
            "all",
            systems,
            int(elevation_lower) if elevation_lower is not None else None,
            int(elevation_upper) if elevation_upper is not None else None,
            '|'.join(habitats_list),
            scientific_name,
            family_name,
            class_name,
            json.dumps(threats),
            category,
            category_weight,
            geometry,
            include_in_star,
          ]],
        columns=COLUMNS,
        crs=target_projection or 'epsg:4326'
    )
    tidy_reproject_save(gdf, report, output_directory_path, target_projection)
    return report

def apply_overrides(
    overrides_path: Path,
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
    overrides_path: Optional[Path],
    excludes_path: Optional[Path],
    output_directory_path: Path,
    target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    cursor = connection.cursor()

    excludes: tuple = tuple([])
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
        era_output_directory_path = output_directory_path / era

        # You can't do NOT IN on an empty list in SQL
        if excludes:
            exclude_statement = "AND assessments.sis_taxon_id NOT IN %s"
            statement = MAIN_STATEMENT + exclude_statement
            cursor.execute(statement, (class_name, excludes))
        else:
            cursor.execute(MAIN_STATEMENT, (class_name,))

        # This can be quite big (tens of thousands), but in modern computer term is quite small
        # and I need to make a follow on DB query per result.
        results = cursor.fetchall()

        logger.info("Found %d species in class %s in scenarion %s", len(results), class_name, era)

        if overrides_path:
            results = apply_overrides(overrides_path, results)

        # The limiting amount here is how many concurrent connections the database can take
        try:
            with Pool(processes=20) as pool:
                reports = pool.map(
                    partial(process_row, class_name, era_output_directory_path, target_projection, presence),
                    results
                )
        except psycopg2.OperationalError:
            sys.exit("Database connection failed for some rows, aborting")

        reports_df = pd.DataFrame(
            [x.as_row() for x in reports],
            columns=SpeciesReport.REPORT_COLUMNS
        ).sort_values('id_no')
        os.makedirs(era_output_directory_path, exist_ok=True)
        reports_df.to_csv(era_output_directory_path / "report.csv", index=False)

@snakemake_compatible(mapping={
    "classname": "params.classname",
    "overrides": "params.overrides",
    "excludes": "params.excludes",
    "output_directory_path": "params.output_dir",
    "target_projection": "params.projection",
})
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
        type=Path,
        help="CSV of overrides",
        required=False,
        default=None,
        dest="overrides",
    )
    parser.add_argument(
        '--excludes',
        type=Path,
        help="CSV of taxon IDs to not include",
        required=False,
        default=None,
        dest="excludes"
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Directory where per species GeoJSON is stored',
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
