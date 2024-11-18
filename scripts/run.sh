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

# declare -a TAXALIST=("AMPHIBIA", "AVES", "MAMMALIA", "REPTILIA")
declare -a TAXALIST=("AVES")

# Get habitat layer and prepare for use
reclaimer zenodo --zenodo_id 3939050 --filename PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif --output ${DATADIR}/habitat/raw.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/raw.tif \
                                            --scale 1000.0 \
                                            --projection "ESRI:54009" \
                                            --output ${DATADIR}/habitat_layers/current

python3 ./prepare_layers/make_masks.py --habitat_layers ${DATADIR}/habitat_layers/current \
                                       --output_directory ${DATADIR}/masks

# Fetch and prepare the elevation layers
reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output ${DATADIR}/elevation.tif
gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-max-1k.tif
gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-min-1k.tif

# Generate the crosswalk table
python3 ./prepare_layers/convert_crosswalk.py --original ${PWD}/data/crosswalk_bin_T.csv --output ${DATADIR}/crosswalk.csv

# Get species data per taxa from IUCN data
for TAXA in "${TAXALIST[@]}"
do
    python3 ./prepare_species/extract_species_data_psql.py --class ${TAXA} --output ${DATADIR}/species-info/${TAXA}/ --projection "ESRI:54009"
done

python3 ./utils/aoh_generator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/aohbatch.csv

littlejohn -j 200 -o ${DATADIR}/aohbatch.log -c ${DATADIR}/aohbatch.csv ${VIRTUAL_ENV}/bin/python3 -- ./aoh-calculator/aohcalc.py

# Calculate predictors from AoHs
python3 ./aoh-calculator/summaries/species_richness.py --aohs_folder ${DATADIR}/aohs/current/ \
                                                       --output ${DATADIR}/predictors/species_richness.tif
python3 ./aoh-calculator/summaries/endemism.py --aohs_folder ${DATADIR}/aohs/current/ \
                                               --species_richness ${DATADIR}/predictors/species_richness.tif \
                                               --output ${DATADIR}/predictors/
