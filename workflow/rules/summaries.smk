# STAR Pipeline - Summary Statistics Rules
# =========================================
#
# These rules generate summary statistics from AOH rasters:
#   - Species richness: count of species per pixel
#   - Endemism: weighted measure of endemic species per pixel
#
# Code-sensitive: These rules rebuild if any AOH changes or if the
# aoh package version changes.

import os
from pathlib import Path


# =============================================================================
# Species Richness
# =============================================================================

rule species_richness:
    """
    Calculate species richness from all AOH rasters.

    Species richness is the sum of all species AOHs - giving the number
    of species present at each pixel.

    This rebuilds if:
    - Any AOH file changes
    - The aoh package version changes
    """
    input:
        # All AOHs must be complete
        aoh_sentinel=DATADIR / "validation" / "aohs.csv",
        # Version tracking
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        richness=DATADIR / "summaries" / "species_richness.tif",
    params:
        aohs_folder=DATADIR / "aohs" / SCENARIO,
    log:
        DATADIR / "logs" / "species_richness.log",
    shell:
        """
        mkdir -p $(dirname {output.richness})
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

    This rebuilds if:
    - Species richness changes
    - Any AOH file changes
    - The aoh package version changes
    """
    input:
        # Dependencies
        aoh_sentinel=DATADIR / "validation" / "aohs.csv",
        species_richness=DATADIR / "summaries" / "species_richness.tif",
        # Version tracking
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
