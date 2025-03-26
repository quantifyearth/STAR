import argparse
import json
import os
import sys

import geopandas as gpd
from pyogrio.errors import DataSourceError
from yirgacheffe.layers import RasterLayer

def threat_processing_per_species(
    species_data_path: str,
    aoh_path: str,
    output_directory_path: str,
) -> None:
    try:
        data = gpd.read_file(species_data_path)
    except DataSourceError:
        sys.exit(f"Failed to read {species_data_path}")

    with RasterLayer.layer_from_file(aoh_path) as aoh:

        os.makedirs(output_directory_path, exist_ok=True)

        taxon_id = data.id_no[0]
        category_weight = int(data.category_weight[0])
        threat_data = json.loads(data.threats[0])

        try:
            aoh_base, _ = os.path.splitext(aoh_path)
            aoh_data_path = aoh_base + ".json"
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

            threat_dir_path = os.path.join(output_directory_path, str(threat_id))
            os.makedirs(threat_dir_path, exist_ok=True)
            output_path = os.path.join(threat_dir_path, f"{taxon_id}.tif")
            with RasterLayer.empty_raster_layer_like(aoh, filename=output_path) as result:
                per_threat_per_species_score.save(result)

def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate per species threat layers")
    parser.add_argument(
        '--speciesdata',
        type=str,
        help="Single species/seasonality geojson.",
        required=True,
        dest="species_data_path"
    )
    parser.add_argument(
        '--aoh',
        type=str,
        help="AoH raster  of speices.",
        required=True,
        dest="aoh_path"
    )
    parser.add_argument(
        '--output',
        type=str,
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
