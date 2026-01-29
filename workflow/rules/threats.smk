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
#
# Code-sensitive: These rules rebuild if threat scripts change.

import os
import json
from pathlib import Path


# =============================================================================
# Version Sentinel for Threat Code
# =============================================================================

rule threat_version_sentinel:
    """
    Create a version sentinel that tracks changes to threat processing code.
    """
    input:
        script1=SRCDIR / "threats" / "threat_processing.py",
        script2=SRCDIR / "threats" / "threat_summation.py",
    output:
        sentinel=DATADIR / ".sentinels" / "threat_code_version.txt",
    run:
        import hashlib
        import subprocess

        os.makedirs(os.path.dirname(output.sentinel), exist_ok=True)

        # Hash the tracked scripts
        hashes = []
        for f in input:
            with open(f, 'rb') as fh:
                hashes.append(hashlib.sha256(fh.read()).hexdigest()[:12])

        # Get aoh package version (threat processing depends on it)
        try:
            result = subprocess.run(
                ["aoh-calc", "--version"],
                capture_output=True, text=True, check=True
            )
            aoh_version = result.stdout.strip()
        except Exception:
            aoh_version = "unknown"

        with open(output.sentinel, 'w') as f:
            f.write(f"scripts: {','.join(hashes)}\n")
            f.write(f"aoh: {aoh_version}\n")


# =============================================================================
# Helper Functions
# =============================================================================

def get_star_species_for_taxa(wildcards):
    """
    Get species IDs that should be included in STAR for a taxa.
    A species is included if:
    - It has in_star=true in its GeoJSON
    - Its AOH raster exists (some species have no AOH due to no habitat overlap)
    """
    # Wait for the checkpoint
    checkpoint_output = checkpoints.extract_species_data.get(taxa=wildcards.taxa).output[0]
    geojson_dir = Path(checkpoint_output).parent

    star_species = []
    for geojson_path in geojson_dir.glob("*.geojson"):
        species_id = geojson_path.stem
        aoh_path = DATADIR / "aohs" / SCENARIO / wildcards.taxa / f"{species_id}_all.tif"

        # Check if species should be in STAR and has an AOH
        try:
            with open(geojson_path, 'r') as f:
                data = json.load(f)
            if data['features'][0]['properties'].get('in_star', False):
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
        species_data=DATADIR / "species-info" / "{taxa}" / SCENARIO / "{species_id}.geojson",
        aoh=DATADIR / "aohs" / SCENARIO / "{taxa}" / "{species_id}_all.tif",
        # Version tracking
        version_sentinel=DATADIR / ".sentinels" / "threat_code_version.txt",
    output:
        # Sentinel file since actual outputs depend on species' threats
        sentinel=DATADIR / "threat_rasters" / "{taxa}" / ".{species_id}_complete",
    params:
        output_dir=lambda wildcards: DATADIR / "threat_rasters" / wildcards.taxa,
    log:
        DATADIR / "logs" / "threats" / "{taxa}" / "{species_id}.log",
    resources:
        threat_slots=1,
    shell:
        """
        mkdir -p $(dirname {log})
        python3 {SRCDIR}/threats/threat_processing.py \
            --speciesdata {input.species_data} \
            --aoh {input.aoh} \
            --output {params.output_dir} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Per-Taxa Threat Raster Aggregation
# =============================================================================

rule aggregate_threat_rasters_per_taxa:
    """
    Aggregate rule that ensures all threat rasters for a taxa are generated.
    Only processes species with in_star=true.
    """
    input:
        # AOHs must be complete first
        aoh_sentinel=DATADIR / "aohs" / SCENARIO / "{taxa}" / ".complete",
        # All threat rasters for STAR species
        sentinels=get_threat_raster_sentinels_for_taxa,
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
            str(DATADIR / "threat_rasters" / "{taxa}" / ".complete"),
            taxa=TAXA
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

    Uses internal parallelism via the -j flag.
    """
    input:
        # All threat rasters must be complete
        sentinel=DATADIR / "threat_rasters" / ".all_complete",
        # Version tracking
        version_sentinel=DATADIR / ".sentinels" / "threat_code_version.txt",
    output:
        # Final STAR output
        star_map=DATADIR / "threat_results" / "level0" / "top.tif",
        # Sentinel for completion
        sentinel=DATADIR / "threat_results" / ".complete",
    params:
        threat_rasters_dir=DATADIR / "threat_rasters",
        output_dir=DATADIR / "threat_results_tmp",
        final_dir=DATADIR / "threat_results",
    threads: 4
    log:
        DATADIR / "logs" / "threat_summation.log",
    shell:
        """
        python3 {SRCDIR}/threats/threat_summation.py \
            --threat_rasters {params.threat_rasters_dir} \
            --output {params.output_dir} \
            -j {threads} \
            2>&1 | tee {log}

        # Atomic move of completed directory
        rm -rf {params.final_dir}
        mv {params.output_dir} {params.final_dir}
        touch {output.sentinel}
        """
