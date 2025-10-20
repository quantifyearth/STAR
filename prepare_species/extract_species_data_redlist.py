#!/usr/bin/env python3
"""
Extract species data from IUCN Red List spatial data + API.

This script combines two data sources:
1. IUCN Red List spatial data (shapefiles) - for range geometries with presence/origin filtering
2. IUCN Red List API v4 (via redlistapi package) - for assessment data (threats, habitats, elevation)

REQUIRED SETUP:
==============

1. Download IUCN Red List Spatial Data:
   - Go to: https://www.iucnredlist.org/resources/spatial-data-download
   - Create an account (free for non-commercial use)
   - Download the shapefile for your target taxonomic group(s):
     * MAMMALS
     * AMPHIBIANS
     * REPTILES
     * BIRDS
   - Extract the shapefiles to a known location on your computer
   - Note: Each download contains a shapefile (.shp) and associated files (.dbf, .shx, .prj)

2. Get an IUCN Red List API Token:
   - Go to: https://api.iucnredlist.org/users/sign_up
   - Sign up for a free API account
   - You'll receive an API token via email
   - Set the token as an environment variable:
     export IUCN_REDLIST_TOKEN="your_token_here"

USAGE:
======
python3 extract_species_data_redlist.py \
    --shapefile /path/to/MAMMALS_DIRECTORY \
    --class MAMMALIA \
    --output /path/to/output/species-info/MAMMALIA/ \
    --projection "ESRI:54009" \
    --excludes /path/to/SpeciesList_generalisedRangePolygons.csv

Or for a single shapefile:
python3 extract_species_data_redlist.py \
    --shapefile /path/to/MAMMALS.shp \
    --class MAMMALIA \
    --output /path/to/output/species-info/MAMMALIA/

The script will:
- Read shapefile(s) and filter by presence (1, 2) and origin (1, 2, 6)
- If a directory is provided, load and merge all .shp files in that directory
- Query the IUCN API for each species to get threats, habitats, elevation
- Merge the data and save per-species GeoJSON files
- Generate a report CSV showing what data was successfully retrieved
"""

import argparse
import json
import logging
import os
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import redlistapi
import requests.exceptions
import shapely

from common import (
    CATEGORY_WEIGHTS,
    COLUMNS,
    process_habitats,
    process_threats,
    process_systems,
    tidy_reproject_save,
    SpeciesReport,
)

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)


def process_systems_from_api(assessment: dict, report: SpeciesReport) -> str:
    """Extract and validate systems data from API response."""
    # The assessment_as_pandas() returns terrestrial, freshwater, marine as boolean columns
    systems_list = []
    if assessment['terrestrial']:
        systems_list.append("Terrestrial")
    if assessment['freshwater']:
        systems_list.append("Freshwater")
    if assessment['marine']:
        systems_list.append("Marine")

    systems = "|".join(systems_list)
    return process_systems([[systems]], report)

def process_threats_from_api(assessment: dict, report: SpeciesReport) -> list:
    """Extract and process threat data from API, applying STAR weighting."""
    threats_data = assessment.get('threats', [])

    # API uses underscores (e.g., "2_3_2") but we need dots (e.g., "2.3.2") for consistency with DB format
    threats = [(
        threat.get('code', '').replace('_', '.'),
        threat.get('scope'),
        threat.get('severity'),
    ) for threat in threats_data]

    return process_threats(threats, report)

def process_habitats_from_api(assessment: dict, report: SpeciesReport) -> set:
    """Extract habitat codes from API response."""
    habitats_data = assessment.get('habitats', [])
    # API uses underscores (e.g., "1_5") but we need dots (e.g., "1.5") for consistency with DB format
    # Convert codes and join with pipe separator to match DB format
    codes_list = [habitat.get('code', '').replace('_', '.') for habitat in habitats_data]
    codes = [['|'.join(codes_list)]]
    return process_habitats(codes, report)

def get_elevation_from_api(assessment: dict, report: SpeciesReport) -> tuple:
    """Extract elevation limits from API response."""
    supplementary_info = assessment.get('supplementary_info', {})

    elevation_lower = supplementary_info.get('lower_elevation_limit')
    elevation_upper = supplementary_info.get('upper_elevation_limit')

    if elevation_lower is not None or elevation_upper is not None:
        report.has_elevation = True

    if elevation_lower is not None:
        elevation_lower = int(elevation_lower)
    if elevation_upper is not None:
        elevation_upper = int(elevation_upper)

    return elevation_lower, elevation_upper

def process_species(
    token: str,
    class_name: str,
    output_directory_path: Path,
    target_projection: Optional[str],
    base_presence_filter: tuple[int, ...],
    species_data: tuple,
) -> SpeciesReport:
    """
    Process a single species, combining all its range geometries.

    This function:
    1. Queries the IUCN API for assessment data
    2. Determines correct presence filter based on possibly_extinct status
    3. Filters and unions all geometry polygons for the species
    4. Merges both data sources
    5. Saves the result as a GeoJSON file
    """

    id_no, species_gdf = species_data
    scientific_name = species_gdf.iloc[0]['scientific_name']

    report = SpeciesReport(id_no, None, scientific_name)

    logger.info("Processing %s (%s)", id_no, scientific_name)

    # Get assessment from API. The Redlistapi package returns the most recent assessment when you
    # use `from_taxid`
    try:
        factory = redlistapi.AssessmentFactory(token)
        assessment = factory.from_taxid(id_no)
        report.has_api_data = True
    except (requests.exceptions.RequestException, ValueError) as e:
        logger.error("Failed to get API data for %s: %s", scientific_name, e)
        return report

    # Whilst you can do `assessment.assessment` to get the original data as a dict,
    # there is a bunch of data cleaning that is done as part of `assessment_as_pandas` which
    # is nice to have, so we call that and then covert it back to a dict for Python
    # convenience.
    assessment_dict = assessment.assessment_as_pandas().to_dict(orient='records')[0]

    try:
        assessment_id = assessment_dict['assid']
        assessment_year = assessment_dict['assessment_date'].year
        category = assessment_dict['red_list_category']
        family_name = assessment_dict['family_name']
        possibly_extinct = assessment_dict['possibly_extinct']
        possibly_extinct_in_the_wild = assessment_dict['possibly_extinct_in_the_wild']
    except KeyError as exc:
        logger.error("Failed to get data from assessment record for %s: %s", id_no, exc)
        return report

    report.assessment_id = assessment_id

    # From Chess STAR report: adjust presence filter for possibly extinct species
    presence_filter = base_presence_filter
    if possibly_extinct or possibly_extinct_in_the_wild:
        presence_filter = presence_filter + (4,)
        report.possibly_extinct = True

    # Only process species in threat categories
    if category not in CATEGORY_WEIGHTS:
        logger.debug("Dropping %s: category %s not in %s", id_no, category, list(CATEGORY_WEIGHTS.keys()))
        return report

    # Process systems
    try:
        systems = process_systems_from_api(assessment_dict, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, exc)
        return report

    # Process threats
    threats = process_threats_from_api(assessment_dict, report)
    if len(threats) == 0:
        logger.debug("Dropping %s: no threats", id_no)
        return report

    # Process habitats
    habitats = process_habitats_from_api(assessment_dict, report)

    # Get elevation
    elevation_lower, elevation_upper = get_elevation_from_api(assessment_dict, report)

    # Now filter and union geometries based on the correct presence filter
    # Filter by presence codes (now that we know if species is possibly extinct)
    filtered_gdf = species_gdf[species_gdf['presence'].isin(presence_filter)]

    if len(filtered_gdf) == 0:
        logger.debug("Dropping %s: no geometries after presence filtering", id_no)
        return report

    geometries = []
    for _, row in filtered_gdf.iterrows():
        geom = row.geometry
        if geom is not None and not geom.is_empty:
            if not geom.is_valid:
                geom = geom.buffer(0)
            geometries.append(geom)

    if len(geometries) == 0:
        logger.debug("Dropping %s: no valid geometries", id_no)
        return report

    report.has_geometry = True

    # Union all geometries
    if len(geometries) == 1:
        geometry = geometries[0]
    else:
        geometry = shapely.union_all(geometries)

    # Create GeoDataFrame with all data
    gdf = gpd.GeoDataFrame(
        [[
            id_no,
            assessment_id,
            int(assessment_year) if assessment_year else None,
            "all",  # season
            systems,
            elevation_lower,
            elevation_upper,
            '|'.join(list(habitats)),
            scientific_name,
            family_name,
            class_name,
            json.dumps(threats),
            category,
            CATEGORY_WEIGHTS[category],
            geometry
        ]],
        columns=COLUMNS,
        crs='EPSG:4326'
    )

    # Save to file
    tidy_reproject_save(gdf, report, output_directory_path, target_projection)

    return report


def extract_data_from_shapefile(
    shapefile_path: Path,
    class_name: str,
    token: str,
    excludes_path: Optional[Path],
    output_directory_path: Path,
    target_projection: Optional[str],
    presence_filter: tuple[int, ...],
    origin_filter: tuple[int, ...],
) -> None:
    """
    Extract species data from IUCN shapefile(s) combined with API data.

    Args:
        shapefile_path: Path to IUCN Red List shapefile or directory containing shapefiles
        class_name: Taxonomic class (e.g., "MAMMALIA")
        token: IUCN API token
        excludes_path: Optional CSV of species IDs to exclude
        output_directory_path: Where to save output files
        target_projection: Target CRS for output
        presence_filter: Presence codes to include (default: 1, 2)
        origin_filter: Origin codes to include (default: 1, 2, 6)
    """

    if shapefile_path.is_dir():
        shapefiles = list(shapefile_path.glob("*.shp"))
        if len(shapefiles) == 0:
            raise ValueError(f"No shapefiles found in directory: {shapefile_path}")

        logger.info("Found %d shapefile(s) in %s", len(shapefiles), shapefile_path)

        gdfs = []
        for shp_file in shapefiles:
            gdf_part = gpd.read_file(shp_file)
            gdfs.append(gdf_part)

        gdf = pd.concat(gdfs, ignore_index=True)
        logger.info("Combined %d total rows from %d shapefile(s)", len(gdf), len(shapefiles))
    else:
        gdf = gpd.read_file(shapefile_path)
        logger.info("Loaded %d rows from shapefile", len(gdf))


    # Normalise column names (shapefiles may have different conventions)
    column_map = {}
    for col in gdf.columns:
        col_lower = col.lower()
        if col_lower in ['binomial', 'sci_name', 'scientific_name']:
            column_map[col] = 'scientific_name'
        elif col_lower in ['id_no', 'sisid', 'sis_id']:
            column_map[col] = 'id_no'
        elif col_lower in ['presence', 'pres']:
            column_map[col] = 'presence'
        elif col_lower in ['origin', 'orig']:
            column_map[col] = 'origin'

    gdf = gdf.rename(columns=column_map)

    # Filter by origin (but NOT presence yet - we need to check possibly_extinct first)
    if 'origin' in gdf.columns:
        before = len(gdf)
        gdf = gdf[gdf['origin'].isin(origin_filter)]
        logger.info("Filtered by origin %s: %d -> %d rows", origin_filter, before, len(gdf))
    else:
        raise ValueError("No 'origin' column found in shapefile - cannot filter by origin")

    if 'presence' not in gdf.columns:
        raise ValueError("Shapefile must have a 'presence' column")

    excludes = set()
    if excludes_path:
        try:
            df = pd.read_csv(excludes_path)
            excludes = set(df.id_no.unique())
            logger.info("Excluding %d species from %s", len(excludes), excludes_path)
        except (FileDoesNotExist, pd.errors.ParserError, KeyError, ValueError) as e:
            raise ValueError("Could not load excludes file: %s", e)

    if excludes:
        before = len(gdf)
        gdf = gdf[~gdf['id_no'].isin(excludes)]
        logger.info("After excluding species: %d -> %d shapes", before, len(gdf))

    # Group by species
    # The shapefile has multiple rows per species (one per range polygon)
    # We'll pass all rows for each species to the processing function so it can
    # apply the correct presence filter based on possibly_extinct status
    species_groups = []
    for id_no, group in gdf.groupby('id_no'):
        species_groups.append((id_no, group))

    logger.info("Grouped into %d unique species", len(species_groups))

    era = "current"
    era_output_directory_path = output_directory_path / era

    logger.info("Processing %d species for %s in %s scenario", len(species_groups), class_name, era)

    # Process species - using multiprocessing
    # Note: We use fewer processes than the DB version because we're making API calls
    with Pool(processes=5) as pool:
        reports = pool.map(
            partial(
                process_species,
                token,
                class_name,
                era_output_directory_path,
                target_projection,
                presence_filter,
            ),
            species_groups
        )
    # reports = [
    #     process_species(token, class_name, era_output_directory_path, target_projection, presence_filter, species)
    #     for species in species_groups
    # ]

    reports_df = pd.DataFrame(
        [x.as_row() for x in reports],
        columns=SpeciesReport.REPORT_COLUMNS
    ).sort_values('id_no')

    os.makedirs(era_output_directory_path, exist_ok=True)
    reports_df.to_csv(era_output_directory_path / "report.csv", index=False)

    logger.info("Saved report to %s", era_output_directory_path / 'report.csv')

    total = len(reports)
    with_files = reports_df['filename'].notna().sum()
    logger.info("Successfully processed %d/%d species", with_files, total)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract species data from IUCN Red List shapefile + API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--shapefile',
        type=Path,
        help="Path to IUCN Red List shapefile (e.g., MAMMALS.shp) or directory containing shapefiles",
        required=True,
    )
    parser.add_argument(
        '--class',
        type=str,
        help="Species class name (e.g., MAMMALIA, AMPHIBIA, REPTILIA, AVES)",
        required=True,
        dest="classname",
    )
    parser.add_argument(
        '--excludes',
        type=Path,
        help="CSV of taxon IDs to exclude (SpeciesList_generalisedRangePolygons.csv)",
        required=False,
        dest="excludes"
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Directory where per species GeoJSON files will be stored',
        required=True,
        dest='output_directory_path',
    )
    parser.add_argument(
        '--projection',
        type=str,
        help="Target projection (default: ESRI:54009)",
        required=False,
        dest="target_projection",
        default="ESRI:54009"
    )
    parser.add_argument(
        '--presence',
        type=str,
        help="Comma-separated presence codes to include (default: 1,2)",
        default="1,2",
    )
    parser.add_argument(
        '--origin',
        type=str,
        help="Comma-separated origin codes to include (default: 1,2,6)",
        default="1,2,6",
    )

    args = parser.parse_args()

    # Get API token
    token = os.getenv('REDLIST_API_TOKEN')
    if not token:
        print("ERROR: REDLIST_API_TOKEN environment variable not set")
        print("Get a token from: https://api.iucnredlist.org/users/sign_up")
        print("Then set it with: export REDLIST_API_TOKEN='your_token_here'")
        sys.exit(1)

    # Parse presence and origin filters
    presence_filter = tuple(int(x) for x in args.presence.split(','))
    origin_filter = tuple(int(x) for x in args.origin.split(','))

    # Verify shapefile or directory exists
    if not args.shapefile.exists():
        print(f"ERROR: Path not found: {args.shapefile}")
        print("\nDownload shapefiles from: https://www.iucnredlist.org/resources/spatial-data-download")
        sys.exit(1)

    if args.shapefile.is_dir():
        # Check that directory contains at least one .shp file
        shapefiles = list(args.shapefile.glob("*.shp"))
        if len(shapefiles) == 0:
            print(f"ERROR: No .shp files found in directory: {args.shapefile}")
            sys.exit(1)

    extract_data_from_shapefile(
        args.shapefile,
        args.classname,
        token,
        args.excludes,
        args.output_directory_path,
        args.target_projection,
        presence_filter,
        origin_filter,
    )


if __name__ == "__main__":
    main()
