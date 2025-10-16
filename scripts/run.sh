#!/bin/bash
#
# Assumes you've set up a python virtual environement in the current directory.
#
# In addition to the Python environemnt, you will need the following extra command line tools:
#
# https://github.com/quantifyearth/reclaimer - used to download inputs from Zenodo directly
# https://github.com/quantifyearth/littlejohn - used to run batch jobs in parallel

# Set shell script to exit on first error (-e) and to output commands being run to make
# reviewing logs easier (-x)
set -e
set -x

# We know we use two Go tools, so add go/bin to our path as in slurm world they're likely
# to be installed locally
export PATH="${PATH}":"${HOME}"/go/bin
if ! hash littlejohn 2>/dev/null; then
    echo "Please ensure littlejohn is available"
    exit 1
fi
if ! hash reclaimer 2>/dev/null; then
    echo "Please ensure reclaimer is available"
    exit 1
fi

# Detect if we're running under SLURM
if [[ -n "${SLURM_JOB_ID}" ]]; then
    # Slurm users will probably need to customise this
    # shellcheck disable=SC1091
    source "${HOME}"/venvs/star/bin/activate
    cd "${HOME}"/dev/star
    PROCESS_COUNT="${SLURM_JOB_CPUS_PER_NODE}"
else
    PROCESS_COUNT=$(nproc --all)
fi
echo "Using ${PROCESS_COUNT} threads."

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi

if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

declare -a TAXALIST=("AMPHIBIA" "AVES" "MAMMALIA" "REPTILIA")

if [ ! -d "${DATADIR}" ]; then
    mkdir "${DATADIR}"
fi

# Get habitat layer and prepare for use
if [ ! -d "${DATADIR}"/habitat_layers ]; then
    if [ ! -f "${DATADIR}"/habitat/raw.tif ]; then
        echo "Fetching habitat map..."
        reclaimer zenodo --zenodo_id 3939050 \
                         --filename PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif \
                         --output "${DATADIR}"/habitat/raw.tif
    fi

    echo "Processing habitat map..."
    aoh-habitat-process --habitat "${DATADIR}"/habitat/raw.tif \
                        --scale 1000.0 \
                        --projection "ESRI:54009" \
                        --output "${DATADIR}"/tmp_habitat_layers/current
    mv "${DATADIR}"/tmp_habitat_layers "${DATADIR}"/habitat_layers
fi

if [ ! -d "${DATADIR}"/masks ]; then
    echo "Processing masks..."
    python3 ./prepare_layers/make_masks.py --habitat_layers "${DATADIR}"/habitat_layers/current \
                                        --output_directory "${DATADIR}"/masks
fi

# Fetch and prepare the elevation layers
if [[ ! -f "${DATADIR}"/elevation/elevation-max-1k.tif || ! -f "${DATADIR}"/elevation/elevation-min-1k.tif ]]; then
    if [ ! -f "${DATADIR}"/elevation/elevation.tif ]; then
        echo "Fetching elevation map..."
        mkdir -p "${DATADIR}"/elevation
        reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output "${DATADIR}"/elevation/elevation.tif
    fi
    if [ ! -f "${DATADIR}"/elevation/elevation-max-1k.tif ]; then
        echo "Generating elevation max layer..."
        gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 "${DATADIR}"/elevation/elevation.tif "${DATADIR}"/elevation/elevation-max-1k.tif
    fi
    if [ ! -f "${DATADIR}"/elevation/elevation-min-1k.tif ]; then
        echo "Generating elevation min layer..."
        gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 "${DATADIR}"/elevation/elevation.tif "${DATADIR}"/elevation/elevation-min-1k.tif
    fi
fi

# Generate the crosswalk table
if [ ! -f "${DATADIR}"/crosswalk.csv ]; then
    echo "Generating crosswalk table..."
    python3 ./prepare_layers/convert_crosswalk.py --original "${DATADIR}"/crosswalk_bin_T.csv --output "${DATADIR}"/crosswalk.csv
fi

# Get species data per taxa from IUCN data
for TAXA in "${TAXALIST[@]}"
do
    if [ ! -d "${DATADIR}"/species-info/"${TAXA}"/ ]; then
        echo "Extracting species data for ${TAXA}..."
        python3 ./prepare_species/extract_species_data_psql.py --class "${TAXA}" --output "${DATADIR}"/species-info/"${TAXA}"/ --projection "ESRI:54009" --excludes "${DATADIR}"/SpeciesList_generalisedRangePolygons.csv
    fi
done

if [ -f "${DATADIR}"/BL_Species_Elevations_2023.csv ]; then
    echo "Applying birdlife data..."
    python3 ./prepare_species/apply_birdlife_data.py --geojsons "${DATADIR}"/species-info/AVES --overrides "${DATADIR}"/BL_Species_Elevations_2023.csv
fi

echo "Generating AoH task list..."
python3 ./utils/aoh_generator.py --input "${DATADIR}"/species-info --datadir "${DATADIR}" --output "${DATADIR}"/aohbatch.csv

echo "Generating AoHs..."
littlejohn -j "${PROCESS_COUNT}" -o "${DATADIR}"/aohbatch.log -c "${DATADIR}"/aohbatch.csv "${VIRTUAL_ENV}"/bin/aoh-calc

# Calculate predictors from AoHs
echo "Generating species richness..."
aoh-species-richness --aohs_folder "${DATADIR}"/aohs/current/ \
                     --output "${DATADIR}"/summaries/species_richness.tif
echo "Generating endemism..."
aoh-endemism --aohs_folder "${DATADIR}"/aohs/current/ \
             --species_richness "${DATADIR}"/summaries/species_richness.tif \
             --output "${DATADIR}"/summaries/endemism.tif

# Aoh Validation
echo "Collating validation data..."
aoh-collate-data --aoh_results "${DATADIR}"/aohs/current/ \
                 --output "${DATADIR}"/validation/aohs.csv
echo "Calculating model validation..."
aoh-validate-prevalence --collated_aoh_data "${DATADIR}"/validation/aohs.csv \
                        --output "${DATADIR}"/validation/model_validation.csv

# Threats
echo "Generating threat task list..."
python3 ./utils/threats_generator.py --input "${DATADIR}"/species-info --datadir "${DATADIR}" --output "${DATADIR}"/threatbatch.csv

echo "Generating threat rasters..."
littlejohn -j "${PROCESS_COUNT}" -o "${DATADIR}"/threatbatch.log -c "${DATADIR}"/threatbatch.csv "${VIRTUAL_ENV}"/bin/python3 -- ./threats/threat_processing.py

echo "Summarising threats..."
python3 ./threats/threat_summation.py --threat_rasters "${DATADIR}"/threat_rasters --output "${DATADIR}"/threat_results
