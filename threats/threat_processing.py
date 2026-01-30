import argparse
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import yirgacheffe as yg
from pyogrio.errors import DataSourceError
from snakemake_argparse_bridge import snakemake_compatible

def threat_processing_per_species(
    species_data_path: Path,
    aoh_path: Path,
    output_directory_path: Path,
    sentinel_path: Path | None,
) -> None:
    try:
        data = gpd.read_file(species_data_path)
    except DataSourceError:
        sys.exit(f"Failed to read {species_data_path}")

    with yg.read_raster(aoh_path) as aoh:

        os.makedirs(output_directory_path, exist_ok=True)

        taxon_id = data.id_no[0]
        category_weight = int(data.category_weight[0])
        raw_threats = data.threats[0]
        threat_data = json.loads(raw_threats) if isinstance(raw_threats, str) else raw_threats

        try:
            aoh_data_path = aoh_path.with_suffix(".json")
            with open(aoh_data_path, "r", encoding="UTF-8") as f:
                aoh_data = json.load(f)
            aoh_total = aoh_data["aoh_total"]
        except (FileNotFoundError, KeyError):
            aoh_total = aoh.sum()

        proportional_aoh_per_pixel = aoh / aoh_total
        weighted_species = proportional_aoh_per_pixel * category_weight

        total_threat_weight = sum(x[1] for x in threat_data)
        for threat_id, weight in threat_data:
            proportional_threat_weight = weight  / total_threat_weight
            per_threat_per_species_score = weighted_species * proportional_threat_weight

            threat_dir_path = output_directory_path / str(threat_id)
            os.makedirs(threat_dir_path, exist_ok=True)
            output_path = threat_dir_path / f"{taxon_id}.tif"
            per_threat_per_species_score.to_geotiff(output_path)

    # This script generates a bunch of rasters, but snakemake needs one
    # output to say when done, so if we're in snakemake mode we touch a sentinel file to
    # let it know we've done. One day this should be another decorator.
    if sentinel_path is not None:
        os.makedirs(sentinel_path.parent, exist_ok=True)
        sentinel_path.touch()

@snakemake_compatible(mapping={
    "species_data_path": "input.species_data",
    "aoh_path": "input.aoh",
    "output_directory_path": "params.output_dir",
    "sentinel_path": "output.sentinel",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate per species threat layers")
    parser.add_argument(
        '--speciesdata',
        type=Path,
        help="Single species/seasonality geojson.",
        required=True,
        dest="species_data_path"
    )
    parser.add_argument(
        '--aoh',
        type=Path,
        help="AoH raster  of speices.",
        required=True,
        dest="aoh_path"
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Directory where per species/threat layers are stored',
        required=True,
        dest='output_directory_path',
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

    threat_processing_per_species(
        args.species_data_path,
        args.aoh_path,
        args.output_directory_path,
        args.sentinel_path,
    )

if __name__ == "__main__":
    main()
