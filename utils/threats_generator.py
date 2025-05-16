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
    taxas = [x.name for x in taxa_dirs]

    res = []
    for taxa in taxas:
        for scenario in ['current',]:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = os.path.join(input_dir, taxa, source)
            species_infos = os.listdir(taxa_path)
            for species_info_path in species_infos:
                taxon_id, _ = os.path.splitext(species_info_path)
                aoh_path = os.path.join(data_dir, "aohs", source, taxa, f"{taxon_id}_all.tif")
                if os.path.exists(aoh_path):
                    res.append([
                        os.path.join(data_dir, "species-info", taxa, source, species_info_path),
                        aoh_path,
                        os.path.join(data_dir, "threat_rasters", taxa)
                    ])

    df = pd.DataFrame(res, columns=[
        '--speciesdata',
        '--aoh',
        '--output'
    ])
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
