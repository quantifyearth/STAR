#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import pandas as pd

def threats_generator(
    input_dir: str,
    data_dir: str,
    output_csv_path: str
):
    taxa_dirs = Path(input_dir).glob("[!.]*")
    data_dir = Path(data_dir)

    res = []
    for taxa_dir_path in taxa_dirs:
        for scenario in ['current',]:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = taxa_dir_path / source
            species_paths = taxa_path.glob("*.geojson")
            for species_path in species_paths:
                taxon_id = species_path.stem
                aoh_path = data_dir / "aohs" / source / taxa_dir_path.name / f"{taxon_id}_all.tif"
                if aoh_path.exists():
                    res.append([
                        species_path,
                        aoh_path,
                        data_dir / "threat_rasters" / taxa_dir_path.name,
                    ])

    df = pd.DataFrame(res, columns=[
        '--speciesdata',
        '--aoh',
        '--output'
    ])
    output_dir, _ = os.path.split(output_csv_path)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_csv_path, index=False)

def main() -> None:
    parser = argparse.ArgumentParser(description="threat tasts generator.")
    parser.add_argument(
        '--input',
        type=str,
        help="directory with taxa folders of species info",
        required=True,
        dest="input_dir"
    )
    parser.add_argument(
        '--datadir',
        type=str,
        help="directory for results",
        required=True,
        dest="data_dir",
    )
    parser.add_argument(
        '--output',
        type=str,
        help="name of output file for csv",
        required=True,
        dest="output"
    )
    args = parser.parse_args()

    threats_generator(args.input_dir, args.data_dir, args.output)

if __name__ == "__main__":
    main()
