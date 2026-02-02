# STAR Pipeline - Area of Habitat (AoH) Generation Rules
# =======================================================
#
# These rules generate AoH rasters for each species. This is the most
# parallelizable part of the pipeline - each species can be processed
# independently with `snakemake --cores N`.
#
# Code-sensitive: These rules rebuild if the aoh package version changes.

import os
from pathlib import Path


# =============================================================================
# Version Sentinel for AOH Code
# =============================================================================


rule aoh_version_sentinel:
    """
    Create a version sentinel that tracks the aoh package version.
    AOH rules depend on this to trigger rebuilds when the package updates.
    """
    output:
        sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    run:
        import subprocess

        os.makedirs(os.path.dirname(output.sentinel), exist_ok=True)

        # Get aoh package version
        try:
            result = subprocess.run(
                ["aoh-calc", "--version"], capture_output=True, text=True, check=True
            )
            aoh_version = result.stdout.strip()
        except Exception:
            aoh_version = "unknown"

        with open(output.sentinel, "w") as f:
            f.write(f"aoh: {aoh_version}\n")


# =============================================================================
# Per-Species AOH Generation
# =============================================================================


def aoh_species_inputs(wildcards):
    """Return inputs for generate_aoh, including birdlife sentinel for AVES."""
    inputs = {
        "species_data": DATADIR
        / "species-info"
        / wildcards.taxa
        / SCENARIO
        / f"{wildcards.species_id}.geojson",
        # Base layers (precious - won't trigger rebuilds)
        "habitat_sentinel": ancient(
            DATADIR / "habitat_layers" / SCENARIO / ".habitat_complete"
        ),
        "lcc_0": DATADIR / "habitat_layers" / SCENARIO / "lcc_0.tif",
        "crosswalk": DATADIR / "crosswalk.csv",
        "mask": ancient(DATADIR / "masks" / "CGLS100Inland_withGADMIslands.tif"),
        "elevation_max": ancient(DATADIR / config["inputs"]["zenodo_elevation_max"]),
        "elevation_min": ancient(DATADIR / config["inputs"]["zenodo_elevation_min"]),
        # Version sentinel for code-sensitive rebuilds
        "version_sentinel": DATADIR / ".sentinels" / "aoh_version.txt",
    }
    if wildcards.taxa == "AVES":
        inputs["birdlife_applied"] = (
            DATADIR / "species-info" / "AVES" / ".birdlife_applied"
        )
    return inputs


rule generate_aoh:
    """
    Generate Area of Habitat raster for a single species.

    This rule is parallelizable - run with `snakemake --cores N` to process
    multiple species concurrently. Each species takes its GeoJSON data and
    produces a raster (.tif) and metadata (.json) file.

    The rule depends on the aoh version sentinel to rebuild when the
    aoh package is updated.
    """
    input:
        unpack(aoh_species_inputs),
    output:
        # Only declare JSON as output - TIF is optional (not created for empty AOHs)
        metadata=DATADIR / "aohs" / SCENARIO / "{taxa}" / "{species_id}_all.json",
    params:
        habitat_dir=DATADIR / "habitat_layers" / SCENARIO,
    log:
        DATADIR / "logs" / "aoh" / "{taxa}" / "{species_id}_all.log",
    resources:
        # Limit concurrent AOH jobs if needed (e.g., for memory)
        aoh_slots=1,
    shell:
        """
        mkdir -p $(dirname {log})
        aoh-calc \
            --fractional_habitats {params.habitat_dir} \
            --elevation-max {input.elevation_max} \
            --elevation-min {input.elevation_min} \
            --crosswalk {input.crosswalk} \
            --speciesdata {input.species_data} \
            --weights {input.mask} \
            --output $(dirname {output.metadata}) \
            2>&1 | tee {log}
        """


# =============================================================================
# Per-Taxa AOH Aggregation
# =============================================================================


def get_species_ids_for_taxa(wildcards):
    """
    Get all species IDs for a taxa by reading the checkpoint output.
    Returns list of species IDs (GeoJSON file stems).
    """
    # Wait for the checkpoint to complete
    checkpoint_output = checkpoints.extract_species_data.get(
        taxa=wildcards.taxa
    ).output[0]
    geojson_dir = Path(checkpoint_output).parent
    return [p.stem for p in geojson_dir.glob("*.geojson")]


def get_all_aoh_metadata_for_taxa(wildcards):
    """
    Get paths to all AOH metadata files for a taxa.
    """
    species_ids = get_species_ids_for_taxa(wildcards)
    return [
        DATADIR / "aohs" / SCENARIO / wildcards.taxa / f"{sid}_all.json"
        for sid in species_ids
    ]


checkpoint aggregate_aohs_per_taxa:
    """
    Checkpoint that ensures all AOHs for a taxa are generated.
    Creates a sentinel file when complete.

    This is a checkpoint (not a rule) so that downstream rules like
    threat processing can re-evaluate the DAG after AOHs are created,
    allowing them to see which AOH files actually exist.
    """
    input:
        # Only depend on JSON metadata (always created), not TIFs (optional)
        metadata=get_all_aoh_metadata_for_taxa,
    output:
        sentinel=DATADIR / "aohs" / SCENARIO / "{taxa}" / ".complete",
    shell:
        """
        echo "Generated $(echo {input.metadata} | wc -w) AOHs for {wildcards.taxa}"
        touch {output.sentinel}
        """


# =============================================================================
# Collate AOH Data
# =============================================================================


rule collate_aoh_data:
    """
    Collate metadata from all AOH JSON files into a single CSV.

    This reads the .json metadata files that are generated alongside each
    AOH raster. The CSV is used by downstream validation and analysis.

    Note: This depends on AOH JSON files, not raster files, because some
    species may have empty AOHs (no raster) but still have metadata.
    """
    input:
        # All AOHs must be complete
        sentinels=expand(
            str(DATADIR / "aohs" / SCENARIO / "{taxa}" / ".complete"), taxa=TAXA
        ),
        # Version tracking
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        collated=DATADIR / "validation" / "aohs.csv",
    params:
        aoh_results_dir=DATADIR / "aohs" / SCENARIO,
    log:
        DATADIR / "logs" / "collate_aoh_data.log",
    shell:
        """
        mkdir -p $(dirname {output.collated})
        aoh-collate-data \
            --aoh_results {params.aoh_results_dir} \
            --output {output.collated} \
            2>&1 | tee {log}
        """
