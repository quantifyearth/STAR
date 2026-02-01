import argparse
import math
import os
from pathlib import Path

import aoh
import geopandas as gpd
import pandas as pd
from snakemake_argparse_bridge import snakemake_compatible

# Columns from current BirdLife data overrides:
# SIS ID
# Assessment ID
# WBDB ID
# Sequence
# Scientific name
# Common name
# RL Category
# PE
# PEW
# Min altitude (m)
# Max altitude (m)
# Occasional lower elevation
# Occasional upper elevation

def apply_birdlife_data(
    geojson_directory_path: Path,
    overrides_path: Path,
    sentinel_path: Path | None,
) -> None:
    overrides = pd.read_csv(overrides_path, encoding="latin1")

    for _, row in overrides.iterrows():
        if math.isnan(row["Occasional lower elevation"]) and math.isnan(row["Occasional upper elevation"]):
            continue

        path = geojson_directory_path / "AVES" / "current" / f'{row["SIS ID"]}.geojson'
        if not path.exists():
            continue

        species_info = gpd.read_file(path)
        data = species_info.loc[0].copy()

        if not math.isnan(row["Occasional lower elevation"]):
            data.elevation_lower = float(row["Occasional lower elevation"])
        else:
            data.elevation_lower = float(data.elevation_lower)
        if not math.isnan(row["Occasional upper elevation"]):
            data.elevation_upper = float(row["Occasional upper elevation"])
        else:
            data.elevation_upper = float(data.elevation_upper)
        data = aoh.tidy_data(data)

        res = gpd.GeoDataFrame(data.to_frame().transpose(), crs=species_info.crs, geometry="geometry")
        res.to_file(path, driver="GeoJSON")

    # This script modifies the GeoJSON files, but snakemake needs one
    # output to say when this is done, so if we're in snakemake mode we touch a sentinel file to
    # let it know we've done. One day this should be another decorator.
    if sentinel_path is not None:
        os.makedirs(sentinel_path.parent, exist_ok=True)
        sentinel_path.touch()

@snakemake_compatible(mapping={
    "geojson_directory_path": "params.geojson_dir",
    "overrides": "input.overrides",
    "sentinel_path": "output.sentinel",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Process agregate species data to per-species-file.")
    parser.add_argument(
        '--geojsons',
        type=Path,
        help='Directory where per species Geojson is stored',
        required=True,
        dest='geojson_directory_path',
    )
    parser.add_argument(
        '--overrides',
        type=Path,
        help="CSV of overrides",
        required=True,
        dest="overrides",
    )
    parser.add_argument(
        '--sentinel',
        type=Path,
        help='Generate a sentinel file on completion for snakemake to track',
        required=False,
        default=None,
        dest='sentinel_path',
    )
    args = parser.parse_args()

    apply_birdlife_data(
        args.geojson_directory_path,
        args.overrides,
        args.sentinel_path,
    )

if __name__ == "__main__":
    main()
