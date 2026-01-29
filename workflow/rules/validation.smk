# STAR Pipeline - Validation Rules
# =================================
#
# These rules handle validation of the AOH models:
#
# 1. Collate AOH data: Gather metadata from all AOH JSON files
# 2. Model validation: Statistical analysis of AOH models (requires R)
# 3. GBIF validation: Validate against GBIF occurrence data (PRECIOUS - expensive)
#
# The GBIF validation is treated as "precious" - it will only run if the
# output doesn't exist. This is because GBIF downloads can take hours.

import os
from pathlib import Path


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
        aoh_sentinel=DATADIR / "aohs" / SCENARIO / ".all_complete",
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


# =============================================================================
# Model Validation
# =============================================================================

rule model_validation:
    """
    Perform statistical validation of AOH models.

    This runs a statistical analysis of the AOH outputs to assess
    model quality. Requires R with lme4 and lmerTest packages.

    Rebuilds if:
    - Collated AOH data changes
    - AOH package version changes
    """
    input:
        collated=DATADIR / "validation" / "aohs.csv",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        validation=DATADIR / "validation" / "model_validation.csv",
    log:
        DATADIR / "logs" / "model_validation.log",
    shell:
        """
        aoh-validate-prevalence \
            --collated_aoh_data {input.collated} \
            --output {output.validation} \
            2>&1 | tee {log}
        """


# =============================================================================
# GBIF Validation (PRECIOUS)
# =============================================================================
#
# GBIF validation is expensive (hours of download time) and should only be
# regenerated if the output is explicitly deleted. We use 'ancient()' to
# prevent rebuilds due to timestamp changes.
#
# For future: Could add logic to detect new species and only fetch those.

rule fetch_gbif_data:
    """
    Fetch GBIF occurrence data for a taxa.

    PRECIOUS: This rule is expensive (hours of downloads) and will only
    run if the output doesn't exist. It won't rebuild due to code changes.

    Environment variables required:
        GBIF_USERNAME, GBIF_EMAIL, GBIF_PASSWORD
    """
    input:
        # Use ancient() to prevent rebuilds
        collated=ancient(DATADIR / "validation" / "aohs.csv"),
    output:
        # The output is a directory, we use a sentinel
        sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
    params:
        output_dir=DATADIR / "validation" / "occurrences",
    log:
        DATADIR / "logs" / "fetch_gbif_{taxa}.log",
    shell:
        """
        mkdir -p {params.output_dir}
        aoh-fetch-gbif-data \
            --collated_aoh_data {input.collated} \
            --taxa {wildcards.taxa} \
            --output_dir {params.output_dir} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


rule validate_gbif_occurrences:
    """
    Validate AOH models against GBIF occurrence data.

    PRECIOUS: Depends on GBIF data which is expensive to fetch.
    """
    input:
        gbif_sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
        # Use ancient() to prevent rebuilds due to upstream changes
        aoh_sentinel=ancient(DATADIR / "aohs" / SCENARIO / "{taxa}" / ".complete"),
    output:
        validation=DATADIR / "validation" / "occurrences" / "{taxa}.csv",
    params:
        gbif_data=lambda wildcards: DATADIR / "validation" / "occurrences" / wildcards.taxa,
        species_data=lambda wildcards: DATADIR / "species-info" / wildcards.taxa / SCENARIO,
        aoh_results=lambda wildcards: DATADIR / "aohs" / SCENARIO / wildcards.taxa,
    log:
        DATADIR / "logs" / "validate_gbif_{taxa}.log",
    shell:
        """
        aoh-validate-occurrences \
            --gbif_data_path {params.gbif_data} \
            --species_data {params.species_data} \
            --aoh_results {params.aoh_results} \
            --output {output.validation} \
            2>&1 | tee {log}
        """


# =============================================================================
# GBIF Validation Target
# =============================================================================

rule gbif_validation:
    """
    Target rule for running GBIF validation for all taxa.

    WARNING: This is expensive and will download gigabytes of data from GBIF.
    Only run explicitly with: snakemake gbif_validation
    """
    input:
        expand(
            str(DATADIR / "validation" / "occurrences" / "{taxa}.csv"),
            taxa=TAXA
        ),
