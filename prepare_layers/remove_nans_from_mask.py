import argparse
import os
from pathlib import Path

import yirgacheffe as yg

def remove_nans_from_mask(
    input_path: Path,
    output_path: Path,
) -> None:
    os.makedirs(output_path.parent, exist_ok=True)
    with yg.read_raster(input_path) as layer:
        converted = layer.nan_to_num()
        converted.to_geotiff(output_path)

def main() -> None:
    parser = argparse.ArgumentParser(description="Convert NaNs to zeros in mask layers")
    parser.add_argument(
        '--original',
        type=Path,
        help="Original raster",
        required=True,
        dest="original_path",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Destination raster',
        required=True,
        dest='output_path',
    )
    args = parser.parse_args()

    remove_nans_from_mask(
        args.original_path,
        args.output_path
    )

if __name__ == "__main__":
    main()
