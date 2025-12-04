import argparse
from pathlib import Path
from alive_progress import alive_bar # type: ignore

import yirgacheffe as yg
from yirgacheffe.layers import RescaledRasterLayer

def resize_cglo(
    projection: yg.MapProjection,
    cglo_path: Path,
    output_path: Path,
) -> None:
    with yg.read_rasters(list(cglo_path.glob("*.tif"))) as cglo_30:
        rescaled = RescaledRasterLayer(cglo_30, projection, nearest_neighbour=False)
        with alive_bar(manual=True) as bar:
            rescaled.to_geotiff(output_path, parallelism=True, callback=bar)

def make_hybrid_elevation_map(
    fabdem_path: Path,
    fabdem_patch_path: Path,
    cglo_path: Path,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmpdir = output_path.parent

    # The CGLO files are at a different resolution to FABDEM, so we first
    # need to scale them.
    resized_cglo_path = tmpdir / "cglo.tif"
    if not resized_cglo_path.exists():
        # Get the map projection and pixel scale for fabdem
        fabdem_example_tile = list(fabdem_path.glob("*.tif"))[0]
        with yg.read_raster(fabdem_example_tile) as example:
            projection = example.map_projection

        resize_cglo(projection, cglo_path, resized_cglo_path)

    # Now we build up a large group layer, and rely on the fact that
    # Yirgacheffe will render earlier layers over later layers
    file_list = list(fabdem_patch_path.glob("*.tif")) + \
        list(fabdem_path.glob("*.tif")) + \
        [resized_cglo_path]

    full_layer = yg.read_rasters(file_list)

    with alive_bar(manual=True) as bar:
        full_layer.to_geotiff(output_path, parallelism=256, callback=bar)

def main() -> None:
    parser = argparse.ArgumentParser(description="Convert IUCN crosswalk to minimal common format.")
    parser.add_argument(
        '--fabdem_tiles',
        type=Path,
        help="Directory containing original FABDEM tiles",
        required=True,
        dest="fabdem_path",
    )
    parser.add_argument(
        '--fabdem_patch_tiles',
        type=Path,
        help="Directory containing original FABDEM errata tiles",
        required=True,
        dest="fabdem_patch_path",
    )
    parser.add_argument(
        '--cglo_tiles',
        type=Path,
        help="Directory containing missing_cglo tiles",
        required=True,
        dest="cglo_path",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help="Final output raster",
        required=True,
        dest='output_path',
    )
    args = parser.parse_args()

    make_hybrid_elevation_map(
        args.fabdem_path,
        args.fabdem_patch_path,
        args.cglo_path,
        args.output_path,
    )

if __name__ == "__main__":
    main()
