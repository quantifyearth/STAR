#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import pandas as pd

def aoh_generator(
    input_dir: Path,
    data_dir: Path,
    output_csv_path: Path,
):
    taxa_dirs = input_dir.glob("[!.]*")

    res = []
    for taxa_dir_path in taxa_dirs:
        for scenario in ['current',]:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = taxa_dir_path / source
            species_paths = taxa_path.glob("*.geojson")
            for species_path in species_paths:
                res.append([
                    data_dir / "habitat_layers" / scenario,
                    data_dir / "Zenodo" / "FABDEM_1km_max_patched.tif",
                    data_dir / "Zenodo" / "FABDEM_1km_min_patched.tif",
                    data_dir / "crosswalk.csv",
                    species_path,
                    data_dir / "masks" / "CGLS100Inland_withGADMIslands.tif",
                    data_dir / "aohs" / scenario / taxa_dir_path.name,
                ])

    df = pd.DataFrame(res, columns=[
        '--fractional_habitats',
        '--elevation-max',
        '--elevation-min',
        '--crosswalk',
        '--speciesdata',
        '--weights',
        '--output'
    ])
    output_dir, _ = os.path.split(output_csv_path)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_csv_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--input',
        type=Path,
        help="directory with taxa folders of species info",
        required=True,
        dest="input_dir"
    )
    parser.add_argument(
        '--datadir',
        type=Path,
        help="directory for results",
        required=True,
        dest="data_dir",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help="name of output file for csv",
        required=True,
        dest="output"
    )
    args = parser.parse_args()

    aoh_generator(args.input_dir, args.data_dir, args.output)

if __name__ == "__main__":
    main()
