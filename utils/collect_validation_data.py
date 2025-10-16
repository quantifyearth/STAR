import argparse
import os
import shutil
from pathlib import Path

import pandas as pd

def collect_validation_data(
    model_results_path: Path,
    data_dir: Path,
    output_dir: Path,
) -> None:
    model_results = pd.read_csv(model_results_path)
    os.makedirs(output_dir, exist_ok=True)

    outliers = model_results[model_results.outlier is True]
    for _, row in outliers.iterrows():
        taxid = row.id_no
        klass = row.class_name
        shutil.copy(
            os.path.join(data_dir, "species-info", klass, "current", f"{taxid}.geojson"),
            output_dir
        )
        shutil.copy(
            os.path.join(data_dir, "aohs", "current", klass, f"{taxid}_all.tif"),
            output_dir
        )

def main() -> None:
    parser = argparse.ArgumentParser(description="Collected range/AOH for species that failed validation")
    parser.add_argument(
        '--model_results',
        type=Path,
        help="directory with taxa folders of species info",
        required=True,
        dest="model_results_path"
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
        help="name of output directory",
        required=True,
        dest="output"
    )
    args = parser.parse_args()
    collect_validation_data(args.model_results_path, args.data_dir, args.output)

if __name__ == "__main__":
    main()
