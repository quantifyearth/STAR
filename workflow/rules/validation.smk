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


# =============================================================================
# Model Validation
# =============================================================================


rule model_validation:
    """
    Perform statistical validation of AOH models based on Dahal et al.

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
# Occurrence Validation (EXPENSIVE!)
# =============================================================================
#
# GBIF based occurrence validation is expensive (hours of download time) and
# should only be regenerated if the output is explicitly deleted.


rule fetch_gbif_data:
    """
    Fetch GBIF occurrence data for a taxa.

    This rule is expensive (hours of downloads) and will only run if the output
    doesn't exist. It won't rebuild due to code changes.

    Environment variables required:
        GBIF_USERNAME, GBIF_EMAIL, GBIF_PASSWORD
    """
    input:
        collated=ancient(DATADIR / "validation" / "aohs.csv"),
    output:
        sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
    params:
        output_dir=DATADIR / "validation" / "occurrences",
    log:
        DATADIR / "logs" / "fetch_gbif_{taxa}.log",
    shell:
        """
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

    Depends on GBIF data which is expensive to fetch.
    """
    input:
        gbif_sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
        # Use ancient() to prevent rebuilds due to upstream changes
        aoh_sentinel=ancient(DATADIR / "aohs" / SCENARIO / "{taxa}" / ".complete"),
    output:
        validation=DATADIR / "validation" / "occurrences" / "{taxa}.csv",
    params:
        gbif_data=lambda wildcards: DATADIR
        / "validation"
        / "occurrences"
        / wildcards.taxa,
        species_data=lambda wildcards: DATADIR
        / "species-info"
        / wildcards.taxa
        / SCENARIO,
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
# Occurrence Validation Target
# =============================================================================


rule occurrence_validation:
    """
    Target rule for running GBIF validation for all taxa.

    WARNING: This is expensive and will download gigabytes of data from GBIF.
    Only run explicitly with: snakemake occurrence_validation
    """
    input:
        expand(str(DATADIR / "validation" / "occurrences" / "{taxa}.csv"), taxa=TAXA),
