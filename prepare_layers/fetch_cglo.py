# STAR uses FABDEM, which is based on the Copernicus GLO 30 Digital Elevation Model (CGLO), but
# was built at a time when CGLO was missing certain areas of the globe. This script downloads those
# extra areas between 40.7371 to 50.9321 degrees longitude and 37.9296 to 45.7696 latitude.
import argparse
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config

def download_copernicus_dem_tiles(
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
    output_dir: Path,
) -> tuple[list[str],list[str]]:
    output_dir.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client(
        's3',
        endpoint_url='https://opentopography.s3.sdsc.edu',
        config=Config(signature_version=UNSIGNED)
    )

    bucket = 'raster'

    lon_tiles = range(int(min_lon), int(max_lon) + 1)
    lat_tiles = range(int(min_lat), int(max_lat) + 1)

    downloaded = []
    failed = []

    for lat in lat_tiles:
        for lon in lon_tiles:
            lat_prefix = 'N' if lat >= 0 else 'S'
            lon_prefix = 'E' if lon >= 0 else 'W'
            lat_str = f"{abs(lat):02d}"
            lon_str = f"{abs(lon):03d}"

            tile_name = f"Copernicus_DSM_10_{lat_prefix}{lat_str}_00_{lon_prefix}{lon_str}_00_DEM"
            s3_key = f"COP30/COP30_hh/{tile_name}.tif"
            local_path = f"{output_dir}/{tile_name}.tif"

            try:
                print(f"Downloading {tile_name}.tif...")
                s3.download_file(bucket, s3_key, local_path)
                downloaded.append(tile_name)
                print(f"  ✓ Saved to {local_path}")
            except Exception as e: # pylint: disable=W0718
                print(f"  ✗ Failed: {e}")
                failed.append(tile_name)

    print(f"\n{'='*60}")
    print(f"Downloaded: {len(downloaded)} tiles")
    if failed:
        print(f"Failed: {len(failed)} tiles")
        print("Failed tiles:", failed)

    return downloaded, failed

def main() -> None:
    parser = argparse.ArgumentParser(description="Convert IUCN crosswalk to minimal common format.")
    parser.add_argument(
        '--output',
        type=Path,
        help='Destination folder for tiles',
        required=True,
        dest='output_dir',
    )
    args = parser.parse_args()

    min_lon = 40.7371
    max_lon = 50.9321
    min_lat = 37.9296
    max_lat = 45.7696

    download_copernicus_dem_tiles(min_lon, max_lon, min_lat, max_lat, args.output_dir)

if __name__ == "__main__":
    main()
