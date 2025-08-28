import argparse
import os
import sys
from pathlib import Path
from typing import Set

import yirgacheffe as yg
import yirgacheffe.operators as yo

OPEN_SEA_LCC = "lcc_200.tif"
NO_DATA_LCC = "lcc_0.tif"

def prepare_mask(
    layers: Set[Path],
    output_path: Path,
    at_least: bool = True,
) -> None:
    assert layers
    rasters = [yg.read_raster(x) for x in layers]

    calc = rasters[0]
    for r in rasters[1:]:
        calc = calc + r
    if at_least:
        calc = yo.where(calc >= 0.5, 1.0, 0.0)
    else:
        calc = yo.where(calc > 0.5, 1.0, 0.0))

    calc.to_geotiff(output_path, parallelism=128)

def prepare_masks(
    habitat_layers_path: Path,
    output_directory_path: Path,
) -> None:
    os.makedirs(output_directory_path, exist_ok=True)

    layer_files = set(habitat_layers_path.glob("lcc_*.tif"))
    if not layer_files:
        sys.exit(f"Found no habitat layers in {habitat_layers_path}")

    marine_layers = {x for x in layer_files if x.name == OPEN_SEA_LCC}
    terrerstrial_layers = {x for x in layer_files if x.name not in [OPEN_SEA_LCC, NO_DATA_LCC]}

    assert len(marine_layers) == 1
    assert len(terrerstrial_layers) < len(layer_files)

    prepare_mask(
        marine_layers,
        output_directory_path / "marine_mask.tif",
    )

    prepare_mask(
        terrerstrial_layers,
        output_directory_path / "terrestrial_mask.tif",
        at_least=True,
    )



def main() -> None:
    parser = argparse.ArgumentParser(description="Generate terrestrial and marine masks.")
    parser.add_argument(
        '--habitat_layers',
        type=Path,
        help="directory with split and scaled habitat layers",
        required=True,
        dest="habitat_layers"
    )
    parser.add_argument(
        '--output_directory',
        type=Path,
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
