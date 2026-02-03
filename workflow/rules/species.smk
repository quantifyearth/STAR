# STAR Pipeline - Species Data Extraction Rules
# ==============================================
#
# These rules handle extracting species data from the IUCN PostgreSQL database.
# Species extraction is a checkpoint because the number of output files
# (one GeoJSON per species) is only known after extraction completes.


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
        excludes=DATADIR / config["optional_inputs"]["species_excludes"],
    output:
        # The report.csv is the known output; GeoJSON files are dynamic
        report=DATADIR / "species-info" / "{taxa}" / SCENARIO / "report.csv",
    params:
        classname="{taxa}",
        output_dir=lambda wildcards: DATADIR / "species-info" / wildcards.taxa,
        projection=config["projection"],
    resources:
        # Serialise DB access scripts - only one extraction script at a time
        # as it will make many concurrent connections internally
        db_connections=1,
    threads: workflow.cores
    script:
        str(SRCDIR / "prepare_species" / "extract_species_data_psql.py")


# =============================================================================
# BirdLife Elevation Overrides (Optional)
# =============================================================================


rule apply_birdlife_overrides:
    """
    Apply BirdLife elevation data overrides to AVES species.
    """
    input:
        # The report isn't read, but acts as a sentinel that the birds
        # species extraction has completed.
        report=DATADIR / "species-info" / "AVES" / SCENARIO / "report.csv",
        overrides=DATADIR / config["optional_inputs"]["birdlife_elevations"],
    output:
        # Unlike other taxa, the AOH stage needs to wait for the data to be
        # applied, so we use this sentinel to indicate that.
        sentinel=DATADIR / "species-info" / "AVES" / ".birdlife_applied",
    params:
        geojson_dir=DATADIR / "species-info",
    script:
        str(SRCDIR / "prepare_species" / "apply_birdlife_data.py")
