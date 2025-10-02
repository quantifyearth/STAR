import argparse
import importlib
import math
import os

import aoh
import geopandas as gpd
import pandas as pd

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
    geojson_directory_path: str,
    overrides_path: str,
) -> None:
    overrides = pd.read_csv(overrides_path, encoding="latin1")

    for _, row in overrides.iterrows():
        if math.isnan(row["Occasional lower elevation"]) and math.isnan(row["Occasional upper elevation"]):
            continue

        path = os.path.join(geojson_directory_path, "AVES", "current", f'{row["SIS ID"]}.geojson')
        if not os.path.exists(path):
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

def main() -> None:
    parser = argparse.ArgumentParser(description="Process agregate species data to per-species-file.")
    parser.add_argument(
        '--geojsons',
        type=str,
        help='Directory where per species Geojson is stored',
        required=True,
        dest='geojson_directory_path',
    )
    parser.add_argument(
        '--overrides',
        type=str,
        help="CSV of overrides",
        required=True,
        dest="overrides",
    )
    args = parser.parse_args()

    apply_birdlife_data(
        args.geojson_directory_path,
        args.overrides
    )

if __name__ == "__main__":
    main()
