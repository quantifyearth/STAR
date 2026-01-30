# STAR Pipeline - Summary Statistics Rules
# =========================================
#
# These rules generate summary statistics from AOH rasters:
#   - Species richness: count of species per pixel
#   - Endemism: weighted measure of endemic species per pixel


# =============================================================================
# Species Richness
# =============================================================================


rule species_richness:
    """
    Calculate species richness from all AOH rasters.

    Species richness is the sum of all species AOHs - giving the number
    of species present at each pixel.
    """
    input:
        # Species richness doesn't use the aoh.csv file, but it's a
        # good indicator that AOH genration has completed
        aoh_sentinel=DATADIR / "validation" / "aohs.csv",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        richness=DATADIR / "summaries" / "species_richness.tif",
    params:
        aohs_folder=DATADIR / "aohs" / SCENARIO,
    log:
        DATADIR / "logs" / "species_richness.log",
    shell:
        """
        aoh-species-richness \
            --aohs_folder {params.aohs_folder} \
            --output {output.richness} \
            2>&1 | tee {log}
        """


# =============================================================================
# Endemism
# =============================================================================


rule endemism:
    """
    Calculate endemism from AOH rasters and species richness.

    Endemism weights each species by the inverse of its range size,
    giving higher values to pixels with range-restricted species.
    """
    input:
        aoh_sentinel=DATADIR / "validation" / "aohs.csv",
        species_richness=DATADIR / "summaries" / "species_richness.tif",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        endemism=DATADIR / "summaries" / "endemism.tif",
    params:
        aohs_folder=DATADIR / "aohs" / SCENARIO,
    log:
        DATADIR / "logs" / "endemism.log",
    shell:
        """
        aoh-endemism \
            --aohs_folder {params.aohs_folder} \
            --species_richness {input.species_richness} \
            --output {output.endemism} \
            2>&1 | tee {log}
        """
