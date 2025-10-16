import argparse
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import yirgacheffe as yg
from pyogrio.errors import DataSourceError

def threat_processing_per_species(
    species_data_path: Path,
    aoh_path: Path,
    output_directory_path: Path,
) -> None:
    try:
        data = gpd.read_file(species_data_path)
    except DataSourceError:
        sys.exit(f"Failed to read {species_data_path}")

    with yg.read_raster(aoh_path) as aoh:

        os.makedirs(output_directory_path, exist_ok=True)

        taxon_id = data.id_no[0]
        category_weight = int(data.category_weight[0])
        threat_data = json.loads(data.threats[0])

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
        print(threat_data)
        print(total_threat_weight)
        for threat_id, weight in threat_data:
            print(threat_id, weight)
            proportional_threat_weight = weight  / total_threat_weight
            per_threat_per_species_score = weighted_species * proportional_threat_weight
            print(per_threat_per_species_score.sum())

            threat_dir_path = output_directory_path / str(threat_id)
            os.makedirs(threat_dir_path, exist_ok=True)
            output_path = threat_dir_path / f"{taxon_id}.tif"
            per_threat_per_species_score.to_geotiff(output_path)

def main() -> None:
    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"

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
    args = parser.parse_args()

    threat_processing_per_species(
        args.species_data_path,
        args.aoh_path,
        args.output_directory_path,
    )

if __name__ == "__main__":
    main()
