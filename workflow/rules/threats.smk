# STAR Pipeline - Threat Processing Rules
# ========================================
#
# These rules generate per-species threat rasters and aggregate them
# into the final STAR threat maps at different hierarchy levels.
#
# The pipeline:
# 1. For each species marked in_star=true with an AOH:
#    - Generate threat rasters (one per threat the species faces)
# 2. Aggregate threat rasters:
#    - Level 2: Sum by threat code prefix (e.g., 1.1, 1.2)
#    - Level 1: Sum by major threat (e.g., 1, 2)
#    - Level 0: Sum all threats (final STAR map)

import json
from pathlib import Path


# =============================================================================
# Helper Functions
# =============================================================================


def get_star_species_for_taxa(wildcards):
    """
    Get species IDs that should be included in STAR for a taxa.
    A species is included if:
    - It has in_star=true in its GeoJSON
    - Its AOH raster exists (some species have no AOH due to no habitat overlap)

    Note: This function assumes AOHs are already generated. The caller
    (aggregate_threat_rasters_inputs) ensures this via checkpoint dependency.
    """
    # Get species data directory from checkpoint
    checkpoint_output = checkpoints.extract_species_data.get(
        taxa=wildcards.taxa
    ).output[0]
    geojson_dir = Path(checkpoint_output).parent

    star_species = []
    for geojson_path in geojson_dir.glob("*.geojson"):
        species_id = geojson_path.stem
        aoh_path = (
            DATADIR / "aohs" / SCENARIO / wildcards.taxa / f"{species_id}_all.tif"
        )

        # Check if species should be in STAR and has an AOH
        try:
            with open(geojson_path, "r") as f:
                data = json.load(f)
            if data["features"][0]["properties"].get("in_star", False):
                # Only include if AOH TIF actually exists
                if aoh_path.exists():
                    star_species.append(species_id)
        except (json.JSONDecodeError, KeyError, IndexError):
            continue

    return star_species


def get_threat_raster_sentinels_for_taxa(wildcards):
    """
    Get paths to threat raster sentinel files for all STAR species in a taxa.
    """
    species_ids = get_star_species_for_taxa(wildcards)
    return [
        DATADIR / "threat_rasters" / wildcards.taxa / f".{sid}_complete"
        for sid in species_ids
    ]


# =============================================================================
# Per-Species Threat Raster Generation
# =============================================================================


rule generate_threat_rasters:
    """
    Generate threat rasters for a single species.

    Each species may face multiple threats, so this produces multiple
    output files (one per threat). We use a sentinel file to track
    completion since the exact outputs are data-dependent.
    """
    input:
        # Species data and AOH
        species_data=DATADIR
        / "species-info"
        / "{taxa}"
        / SCENARIO
        / "{species_id}.geojson",
        aoh=DATADIR / "aohs" / SCENARIO / "{taxa}" / "{species_id}_all.tif",
    output:
        # Sentinel file since actual outputs depend on species' threats
        sentinel=DATADIR / "threat_rasters" / "{taxa}" / ".{species_id}_complete",
    params:
        output_dir=lambda wildcards: DATADIR / "threat_rasters" / wildcards.taxa,
    log:
        DATADIR / "logs" / "threats" / "{taxa}" / "{species_id}.log",
    resources:
        threat_slots=1,
    script:
        str(SRCDIR / "threats" / "threat_processing.py")


# =============================================================================
# Per-Taxa Threat Raster Aggregation
# =============================================================================


def aggregate_threat_rasters_inputs(wildcards):
    """
    Return inputs for aggregate_threat_rasters_per_taxa.
    Explicitly includes AOH checkpoint to ensure AOHs are generated first.
    """
    # Get the AOH checkpoint output - this forces Snakemake to wait for AOHs
    aoh_checkpoint = checkpoints.aggregate_aohs_per_taxa.get(
        taxa=wildcards.taxa
    ).output[0]

    return {
        "aoh_complete": aoh_checkpoint,
        "sentinels": get_threat_raster_sentinels_for_taxa(wildcards),
    }


rule aggregate_threat_rasters_per_taxa:
    """
    Aggregate rule that ensures all threat rasters for a taxa are generated.
    Only processes species with in_star=true.
    """
    input:
        unpack(aggregate_threat_rasters_inputs),
    output:
        sentinel=DATADIR / "threat_rasters" / "{taxa}" / ".complete",
    shell:
        """
        echo "Generated threat rasters for {wildcards.taxa}"
        touch {output.sentinel}
        """


rule all_threat_rasters:
    """
    Aggregate rule that ensures all threat rasters for all taxa are generated.
    """
    input:
        sentinels=expand(
            str(DATADIR / "threat_rasters" / "{taxa}" / ".complete"), taxa=TAXA
        ),
    output:
        sentinel=DATADIR / "threat_rasters" / ".all_complete",
    shell:
        "touch {output.sentinel}"


# =============================================================================
# Threat Summation
# =============================================================================


rule threat_summation:
    """
    Aggregate all per-species threat rasters into hierarchical threat maps.

    This produces:
    - level2/: Aggregated by threat code (e.g., 1.1.tif, 1.2.tif)
    - level1/: Aggregated by major threat (e.g., 1.tif, 2.tif)
    - level0/top.tif: Final STAR map (all threats summed)
    """
    input:
        sentinel=DATADIR / "threat_rasters" / ".all_complete",
    output:
        star_map=DATADIR / "threat_results" / "level0" / "top.tif",
    params:
        threat_rasters_dir=DATADIR / "threat_rasters",
        output_dir=DATADIR / "threat_results",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "threat_summation.log",
    script:
        str(SRCDIR / "threats" / "threat_summation.py")
