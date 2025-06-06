import argparse
import os
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue, cpu_count
from pathlib import Path
from typing import List

from yirgacheffe.layers import RasterLayer  # type: ignore
from osgeo import gdal

gdal.SetCacheMax(1024 * 1024 * 32)

def worker(
    filename: str,
    result_dir: str,
    input_queue: Queue,
) -> None:
    output_tif = os.path.join(result_dir, filename)

    merged_result = None

    while True:
        path = input_queue.get()
        if path is None:
            break

        with RasterLayer.layer_from_file(path) as partial_raster:
            if merged_result is None:
                merged_result = RasterLayer.empty_raster_layer_like(partial_raster)
                cleaned_raster = partial_raster.nan_to_num()
                cleaned_raster.save(merged_result)
            else:
                calc = merged_result + partial_raster.nan_to_num()
                temp = RasterLayer.empty_raster_layer_like(calc)
                calc.save(temp)
                merged_result = temp

    if merged_result:
        final = RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif)
        merged_result.save(final)

def raster_sum(
    images_list: List[Path],
    output_filename: str,
    processes_count: int
) -> None:
    result_dir, filename = os.path.split(output_filename)
    os.makedirs(result_dir, exist_ok=True)

    with tempfile.TemporaryDirectory() as tempdir:
        with Manager() as manager:
            source_queue = manager.Queue()

            workers = [Process(target=worker, args=(
                f"{index}.tif",
                tempdir,
                source_queue
            )) for index in range(processes_count)]
            for worker_process in workers:
                worker_process.start()

            for file in images_list:
                source_queue.put(file)
            for _ in range(len(workers)):
                source_queue.put(None)

            processes = workers
            while processes:
                candidates = [x for x in processes if not x.is_alive()]
                for candidate in candidates:
                    candidate.join()
                    if candidate.exitcode:
                        for victim in processes:
                            victim.kill()
                        sys.exit(candidate.exitcode)
                    processes.remove(candidate)
                time.sleep(1)

            # here we should have now a set of images in tempdir to merge
            single_worker = Process(target=worker, args=(
                filename,
                result_dir,
                source_queue
            ))
            single_worker.start()
            nextfiles = list(Path(tempdir).glob("*.tif"))
            for file in nextfiles:
                source_queue.put(file)
            source_queue.put(None)

            processes = [single_worker]
            while processes:
                candidates = [x for x in processes if not x.is_alive()]
                for candidate in candidates:
                    candidate.join()
                    if candidate.exitcode:
                        for victim in processes:
                            victim.kill()
                        sys.exit(candidate.exitcode)
                    processes.remove(candidate)
                time.sleep(1)

def reduce_to_next_level(
    rasters_directory: str,
    output_directory: str,
    processes_count: int,
) -> None:

    files = list(Path(rasters_directory).glob("**/*.tif"))
    print(f"total items: {len(files)}")
    if not files:
        sys.exit(f"No files in {rasters_directory}, aborting")

    buckets = {}
    for filename in files:
        code, _ = os.path.splitext(filename.name)
        next_level_threat_id = ".".join(code.split('.')[:-1])
        if not next_level_threat_id:
            next_level_threat_id = "top"
        try:
            buckets[next_level_threat_id].append(filename)
        except KeyError:
            buckets[next_level_threat_id] = [filename]

    print(f"Found {len(buckets)} threats at current level:")
    for code, files in buckets.items():
        target_output = os.path.join(output_directory, f"{code}.tif")
        print(f"processing {code}: {len(files)} items")
        raster_sum(files, target_output, processes_count)

def reduce_from_species(
    rasters_directory: str,
    output_directory: str,
    processes_count: int,
) -> None:

    files = list(Path(rasters_directory).glob("**/*.tif"))
    print(f"total items: {len(files)}")
    if not files:
        sys.exit(f"No files in {rasters_directory}, aborting")

    buckets = {}
    for filename in files:
        threat_code = filename.parts[-2]
        levels = threat_code.split('.')
        assert len(levels) > 1 # in practice all species threats are level 2 or level 3

        match len(levels):
            case 2 | 3:
                code = ".".join(levels[:2])
            case _:
                assert False
        try:
            buckets[code].append(filename)
        except KeyError:
            buckets[code] = [filename]

    print(f"Found {len(buckets)} threats at current level:")
    for code, files in buckets.items():
        target_output = os.path.join(output_directory, f"{code}.tif")
        print(f"processing {code}: {len(files)} items")
        raster_sum(files, target_output, processes_count)


def threat_summation(
    rasters_directory: str,
    output_directory: str,
    processes_count: int,
) -> None:
    os.makedirs(output_directory, exist_ok=True)

    # All these files are at level3 to start with, so first make level2
    print("processing level 2")
    level2_target = os.path.join(output_directory, "level2")
    reduce_from_species(rasters_directory, level2_target, processes_count)

    # Now reduce level2 to level1
    print("processing level 1")
    level1_target = os.path.join(output_directory, "level1")
    reduce_to_next_level(level2_target, level1_target, processes_count)

    # Now build a final top level STAR
    print("processing level 0")
    final_target = os.path.join(output_directory, "level0")
    reduce_to_next_level(level1_target, final_target, processes_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generates the combined, and level 1 and level 2 threat rasters.")
    parser.add_argument(
        "--threat_rasters",
        type=str,
        required=True,
        dest="rasters_directory",
        help="GeoTIFF file containing level three per species threats"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output_directory",
        help="Destination directory file for results."
    )
    parser.add_argument(
        "-j",
        type=int,
        required=False,
        default=round(cpu_count() / 2),
        dest="processes_count",
        help="Number of concurrent threads to use."
    )
    args = parser.parse_args()

    threat_summation(
        args.rasters_directory,
        args.output_directory,
        args.processes_count
    )

if __name__ == "__main__":
    main()
