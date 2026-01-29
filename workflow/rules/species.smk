# STAR Pipeline - Species Data Extraction Rules
# ==============================================
#
# These rules handle extracting species data from the IUCN PostgreSQL database.
# Species extraction is a checkpoint because the number of output files
# (one GeoJSON per species) is only known after extraction completes.
#
# Code-sensitive: These rules should rebuild if the extraction scripts change.

import os
from pathlib import Path


# =============================================================================
# Version Sentinel for Code-Sensitive Dependencies
# =============================================================================

rule species_version_sentinel:
    """
    Create a version sentinel that tracks changes to species extraction code.
    Downstream rules depend on this to trigger rebuilds when code changes.
    """
    input:
        # Track the extraction scripts
        script1=SRCDIR / "prepare_species" / "extract_species_data_psql.py",
        script2=SRCDIR / "prepare_species" / "common.py",
    output:
        sentinel=DATADIR / ".sentinels" / "species_code_version.txt",
    run:
        import hashlib
        import subprocess

        os.makedirs(os.path.dirname(output.sentinel), exist_ok=True)

        # Hash the tracked scripts
        hashes = []
        for f in input:
            with open(f, 'rb') as fh:
                hashes.append(hashlib.sha256(fh.read()).hexdigest()[:12])

        # Get aoh package version
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
# Species Data Extraction (Checkpoint)
# =============================================================================

checkpoint extract_species_data:
    """
    Extract species data from PostgreSQL database.

    This is a checkpoint because the number of output GeoJSON files is only
    known after extraction. Each taxa produces N species files where N is
    determined by the database query.

    Environment variables required:
        DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
    """
    input:
        # Code version sentinel for rebuild tracking
        version_sentinel=DATADIR / ".sentinels" / "species_code_version.txt",
    output:
        # The report.csv is the known output; GeoJSON files are dynamic
        report=DATADIR / "species-info" / "{taxa}" / SCENARIO / "report.csv",
    params:
        classname="{taxa}",
        output_dir=lambda wildcards: DATADIR / "species-info" / wildcards.taxa,
        projection=config["projection"],
        excludes=lambda wildcards: (
            f"--excludes {DATADIR / config['optional_inputs']['species_excludes']}"
            if (DATADIR / config["optional_inputs"]["species_excludes"]).exists()
            else ""
        ),
    log:
        DATADIR / "logs" / "extract_species_{taxa}.log",
    resources:
        # Serialize DB access - only one extraction at a time
        db_connections=1,
    shell:
        """
        python3 {SRCDIR}/prepare_species/extract_species_data_psql.py \
            --class {params.classname} \
            --output {params.output_dir} \
            --projection "{params.projection}" \
            {params.excludes} \
            2>&1 | tee {log}
        """


# =============================================================================
# BirdLife Elevation Overrides (Optional)
# =============================================================================

rule apply_birdlife_overrides:
    """
    Apply BirdLife elevation data overrides to AVES species.

    This rule only runs if the BirdLife elevations file exists.
    It modifies GeoJSON files in-place.
    """
    input:
        report=DATADIR / "species-info" / "AVES" / SCENARIO / "report.csv",
        overrides=DATADIR / config["optional_inputs"]["birdlife_elevations"],
    output:
        sentinel=DATADIR / "species-info" / "AVES" / ".birdlife_applied",
    params:
        geojson_dir=DATADIR / "species-info",
    log:
        DATADIR / "logs" / "apply_birdlife_overrides.log",
    shell:
        """
        python3 {SRCDIR}/prepare_species/apply_birdlife_data.py \
            --geojsons {params.geojson_dir} \
            --overrides {input.overrides} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Aggregation Rule for All Species Data
# =============================================================================

def all_species_reports(wildcards):
    """
    Return paths to all species reports for all taxa.
    """
    return [
        DATADIR / "species-info" / taxa / SCENARIO / "report.csv"
        for taxa in TAXA
    ]


rule all_species_data:
    """
    Aggregate rule that ensures all species data is extracted.
    """
    input:
        reports=all_species_reports,
    output:
        sentinel=DATADIR / "species-info" / ".all_extracted",
    shell:
        "touch {output.sentinel}"
