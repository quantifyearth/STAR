import argparse
import os
import sys
from glob import glob
from typing import Set

import numpy as np
from yirgacheffe.layers import RasterLayer

OPEN_SEA_LCC = "lcc_200.tif"
NO_DATA_LCC = "lcc_0.tif"

def prepare_mask(
    layers: Set[str],
    output_path: str,
    at_least: bool = True,
) -> None:
    assert layers
    rasters = [RasterLayer.layer_from_file(x) for x in layers]

    intersection = RasterLayer.find_intersection(rasters)
    for r in rasters:
        r.set_window_for_intersection(intersection)

    calc = rasters[0]
    for r in rasters[1:]:
        calc = calc + r
    if at_least:
        calc = calc.numpy_apply(lambda a: np.where(a >= 0.5, 1.0, 0.0))
    else:
        calc = calc.numpy_apply(lambda a: np.where(a > 0.5, 1.0, 0.0))

    with RasterLayer.empty_raster_layer_like(rasters[0], filename=output_path) as result:
        calc.parallel_save(result)

def prepare_masks(
    habitat_layers_path: str,
    output_directory_path: str,
) -> None:
    os.makedirs(output_directory_path, exist_ok=True)

    layer_files = set(glob("lcc_*.tif", root_dir=habitat_layers_path))
    if not layer_files:
        sys.exit(f"Found no habitat layers in {habitat_layers_path}")

    marine_layers = layer_files & set([OPEN_SEA_LCC])
    terrerstrial_layers = layer_files - set([OPEN_SEA_LCC, NO_DATA_LCC])

    assert len(marine_layers) == 1
    assert len(terrerstrial_layers) == len(layer_files) - 2

    prepare_mask(
        {os.path.join(habitat_layers_path, x) for x in marine_layers},
        os.path.join(output_directory_path, "marine_mask.tif"),
    )

    prepare_mask(
        {os.path.join(habitat_layers_path, x) for x in terrerstrial_layers},
        os.path.join(output_directory_path, "terrestrial_mask.tif"),
        at_least=True,
    )



def main() -> None:
    parser = argparse.ArgumentParser(description="Generate terrestrial and marine masks.")
    parser.add_argument(
        '--habitat_layers',
        type=str,
        help="directory with split and scaled habitat layers",
        required=True,
        dest="habitat_layers"
    )
    parser.add_argument(
        '--output_directory',
        type=str,
        help="Folder for output mask layers",
        required=True,
        dest="output_directory"
    )
    args = parser.parse_args()

    prepare_masks(
        args.habitat_layers,
        args.output_directory
    )

if __name__ == "__main__":
    main()
