# STAR Pipeline - Prepare Layers Rules
# =====================================
#
# These rules handle the "precious" base layers that are slow to build
# and should only be regenerated if explicitly deleted (not if code changes).
#
# Precious layers:
#   - habitat_layers/current/lcc_*.tif
#   - masks/CGLS100Inland_withGADMIslands.tif
#   - elevation maps (if generated locally)
#
# Normal layers (regenerate if inputs change):
#   - crosswalk.csv

import os
from pathlib import Path


# =============================================================================
# Crosswalk Table (normal dependency tracking)
# =============================================================================

rule convert_crosswalk:
    """
    Convert IUCN crosswalk to minimal common format.
    This is fast to regenerate so uses normal dependency tracking.
    """
    input:
        original=SRCDIR / "data" / "crosswalk_bin_T.csv",
    output:
        crosswalk=DATADIR / "crosswalk.csv",
    log:
        DATADIR / "logs" / "convert_crosswalk.log",
    script:
        str(SRCDIR / "prepare_layers" / "convert_crosswalk.py")


# =============================================================================
# Mask Processing (precious - only if missing)
# =============================================================================

rule remove_nans_from_mask:
    """
    Convert NaNs to zeros in the mask layer.

    This is a precious layer - it will only be built if the output doesn't exist.
    The rule won't trigger rebuilds due to code changes.
    """
    input:
        original=ancient(DATADIR / "Zenodo" / "CGLS100Inland_withGADMIslands.tif"),
    output:
        mask=DATADIR / "masks" / "CGLS100Inland_withGADMIslands.tif",
    log:
        DATADIR / "logs" / "remove_nans_from_mask.log",
    script:
        str(SRCDIR / "prepare_layers" / "remove_nans_from_mask.py")


# =============================================================================
# Habitat Layer Processing (precious - only if missing)
# =============================================================================

rule download_habitat:
    """
    Download raw habitat map from Zenodo.

    This is a precious layer - will only download if output doesn't exist.
    """
    output:
        habitat=DATADIR / "habitat" / "raw.tif",
    params:
        zenodo_id=config["zenodo"]["habitat_id"],
        filename=config["zenodo"]["habitat_filename"],
    log:
        DATADIR / "logs" / "download_habitat.log",
    shell:
        """
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --output {output.habitat} \
                         2>&1 | tee {log}
        """


rule process_habitat:
    """
    Process raw habitat map into per-class layers.

    This is a precious layer - the entire habitat_layers directory should
    only be built if it doesn't exist. We use a sentinel file to track this.
    """
    input:
        habitat=ancient(DATADIR / "habitat" / "raw.tif"),
    output:
        # Sentinel file to indicate habitat processing is complete
        sentinel=DATADIR / "habitat_layers" / SCENARIO / ".habitat_complete",
    params:
        scale=config["habitat_scale"],
        projection=config["projection"],
        output_dir=DATADIR / "habitat_layers" / SCENARIO,
        tmp_dir=DATADIR / "tmp_habitat_layers" / SCENARIO,
    log:
        DATADIR / "logs" / "process_habitat.log",
    shell:
        """
        set -e
        aoh-habitat-process --habitat {input.habitat} \
                           --scale {params.scale} \
                           --projection "{params.projection}" \
                           --output {params.tmp_dir} \
                           2>&1 | tee {log}

        # Atomic move of completed directory
        rm -rf {params.output_dir}
        mv {params.tmp_dir} {params.output_dir}

        # Create sentinel file
        touch {output.sentinel}
        """


rule copy_islands_layer:
    """
    Copy the missing landcover (islands) layer to habitat layers.
    This is lcc_0.tif which represents island areas.
    """
    input:
        islands=ancient(DATADIR / "Zenodo" / "MissingLandcover_1km_cover.tif"),
        # Ensure habitat processing is done first
        habitat_sentinel=DATADIR / "habitat_layers" / SCENARIO / ".habitat_complete",
    output:
        lcc_0=DATADIR / "habitat_layers" / SCENARIO / "lcc_0.tif",
    log:
        DATADIR / "logs" / "copy_islands_layer.log",
    shell:
        """
        cp {input.islands} {output.lcc_0} 2>&1 | tee {log}
        """


# =============================================================================
# Helper rule to get specific habitat layer
# =============================================================================

def get_habitat_layer(wildcards):
    """
    Returns the path to a specific habitat layer.
    lcc_0 is special (islands layer), others come from habitat processing.
    """
    n = int(wildcards.n)
    if n == 0:
        return DATADIR / "habitat_layers" / SCENARIO / "lcc_0.tif"
    else:
        return DATADIR / "habitat_layers" / SCENARIO / f"lcc_{n}.tif"


rule habitat_layer:
    """
    Pseudo-rule to request a specific habitat layer.
    This triggers either the habitat processing or islands copy as needed.
    """
    input:
        layer=get_habitat_layer,
    output:
        # This is a bit of a trick - we declare the output but let the
        # upstream rules actually create it
        touch(DATADIR / "habitat_layers" / SCENARIO / ".lcc_{n}_exists"),
