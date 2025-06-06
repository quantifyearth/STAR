#!/bin/bash
#
# Assumes you've set up a python virtual environement in the current directory.
#
# In addition to the Python environemnt, you will need the following extra command line tools:
#
# https://github.com/quantifyearth/reclaimer - used to download inputs from Zenodo directly
# https://github.com/quantifyearth/littlejohn - used to run batch jobs in parallel

set -e

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi

if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

export CPUS=`getconf _NPROCESSORS_ONLN`
export THREADS=$(($CPUS / 2))
echo "Using $THREADS threads."

declare -a TAXALIST=("AMPHIBIA" "AVES" "MAMMALIA" "REPTILIA")

# Get habitat layer and prepare for use
if [ ! -d ${DATADIR}/habitat_layers ]; then
    if [ ! -f ${DATADIR}/habitat/raw.tif ]; then
        echo "Fetching habitat map..."
        reclaimer zenodo --zenodo_id 3939050 --filename PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif --output ${DATADIR}/habitat/raw.tif
    fi

    echo "Processing habitat map..."
    python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/raw.tif \
                                                --scale 1000.0 \
                                                --projection "ESRI:54009" \
                                                --output ${DATADIR}/tmp_habitat_layers/current
    mv ${DATADIR}/tmp_habitat_layers ${DATADIR}/habitat_layers
fi

if [ ! -d ${DATADIR}/masks ]; then
    echo "Processing masks..."
    python3 ./prepare_layers/make_masks.py --habitat_layers ${DATADIR}/habitat_layers/current \
                                        --output_directory ${DATADIR}/masks
fi

# Fetch and prepare the elevation layers
if [[ ! -f ${DATADIR}/elevation/elevation-max-1k.tif || ! -f ${DATADIR}/elevation/elevation-min-1k.tif ]]; then
    if [ ! -f ${DATADIR}/elevation/elevation.tif ]; then
        echo "Fetching elevation map..."
        reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output ${DATADIR}/elevation/elevation.tif
    fi
    if [ ! -f ${DATADIR}/elevation/elevation-max-1k.tif ]; then
        echo "Generating elevation max layer..."
        gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation/elevation.tif ${DATADIR}/elevation/elevation-max-1k.tif
    fi
    if [ ! -f ${DATADIR}/elevation/elevation-min-1k.tif ]; then
        echo "Generating elevation min layer..."
        gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation/elevation.tif ${DATADIR}/elevation/elevation-min-1k.tif
    fi
fi

# Generate the crosswalk table
if [ ! -f ${DATADIR}/crosswalk.csv ]; then
    echo "Generating crosswalk table..."
    python3 ./prepare_layers/convert_crosswalk.py --original ${PWD}/data/crosswalk_bin_T.csv --output ${DATADIR}/crosswalk.csv
fi

# Get species data per taxa from IUCN data
for TAXA in "${TAXALIST[@]}"
do
    echo "Extracting species data for ${TAXA}..."
    python3 ./prepare_species/extract_species_data_psql.py --class ${TAXA} --output ${DATADIR}/species-info/${TAXA}/ --projection "ESRI:54009" --excludes ${DATADIR}/SpeciesList_generalisedRangePolygons.csv
done

if [ -f data/BL_Species_Elevations_2023.csv ]; then
    echo "Applying birdlife data..."
    python3 ./prepare_species/apply_birdlife_data.py --geojsons ${DATADIR}/species-info/AVES --overrides data/BL_Species_Elevations_2023.csv
fi

echo "Generating AoH task list..."
python3 ./utils/aoh_generator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/aohbatch.csv

echo "Generating AoHs..."
littlejohn -j ${THREADS} -o ${DATADIR}/aohbatch.log -c ${DATADIR}/aohbatch.csv ${VIRTUAL_ENV}/bin/python3 -- ./aoh-calculator/aohcalc.py

# Calculate predictors from AoHs
echo "Generating species richness..."
python3 ./aoh-calculator/summaries/species_richness.py --aohs_folder ${DATADIR}/aohs/current/ \
                                                       --output ${DATADIR}/summaries/species_richness.tif
echo "Generating endemism..."
python3 ./aoh-calculator/summaries/endemism.py --aohs_folder ${DATADIR}/aohs/current/ \
                                               --species_richness ${DATADIR}/summaries/species_richness.tif \
                                               --output ${DATADIR}/summaries/endemism.tif

# Aoh Validation
echo "Collating validation data..."
python3 ./aoh-calculator/validation/collate_data.py --aoh_results ${DATADIR}/aohs/current/ \
                                                    --output ${DATADIR}/validation/aohs.csv
echo "Calculating model validation..."
python3 ./aoh-calculator/validation/validate_map_prevalence.py --collated_aoh_data ${DATADIR}/validation/aohs.csv \
                                                               --output ${DATADIR}/validation/model_validation.csv

# Threats
echo "Generating threat task list..."
python3 ./utils/threats_generator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/threatbatch.csv

echo "Generating threat rasters..."
littlejohn -j ${THREADS} -o ${DATADIR}/threatbatch.log -c ${DATADIR}/threatbatch.csv ${VIRTUAL_ENV}/bin/python3 -- ./threats/threat_processing.py

echo "Summarising threats..."
python3 ./threats/threat_summation.py --threat_rasters ${DATADIR}/threat_rasters --output ${DATADIR}/threat_results
