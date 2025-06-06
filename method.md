---
inputs:
    crosswalk: /data/crosswalk.csv
---
# How to run the pipeline


## Building the environment

### The geospatial compute container

The dockerfile that comes with the repo should be used to run the compute parts of the pipeline.

```
docker build . -tag aohbuilder
```

For use with the [shark pipeline](https://github.com/quantifyearth/shark), we need this block to trigger a build currently:

```shark-build:aohbuilder
((from ghcr.io/osgeo/gdal:ubuntu-small-3.9.2)
(run (network host) (shell "apt-get update -qqy && apt-get -y install python3-pip libpq-dev git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (shell "python3 -m pip config set global.break-system-packages true"))
 (run (network host) (shell "pip install 'numpy<2'"))
 (run (network host) (shell "pip install gdal[numpy]==3.9.2"))
 (copy (src "./aoh-calculator") (dst "/root/"))
 (copy (src "./prepare_layers") (dst "/root/"))
 (copy (src "./prepare_species") (dst "/root/"))
 (copy (src "./requirements.txt") (dst "/root/"))
 (workdir "/root/")
 (run (network host) (shell "pip install --no-cache-dir -r requirements.txt"))
)
```

For the primary data sources we fetch them directly from Zenodo/GitHub to allow for obvious provenance.

```shark-build:reclaimer
((from carboncredits/reclaimer:latest))
```

For the projection changes we use a barebones GDAL container. The reason for this is that these operations are expensive, and we don't want to re-execute them if we update our code.

```shark-build:gdalonly
((from ghcr.io/osgeo/gdal:ubuntu-small-3.9.2))
```

Alternatively you can build your own python virtual env assuming you have everything required. For this you will need at least a GDAL version installed locally, and you may want to update requirements.txt to match the python GDAL bindings to the version you have installed.

```
python3 -m virtualenv ./venv
. ./venv/bin/activate
pip install -r requirements.txt
```

### The PostGIS container

For querying the IUCN data held in the PostGIS database we use a seperate container, based on the standard PostGIS image.

```shark-build:postgis
((from python:3.12-slim)
 (run (network host) (shell "apt-get update -qqy && apt-get -y install libpq-dev gcc git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (network host) (shell "pip install psycopg2 SQLalchemy geopandas"))
 (run (network host) (shell "pip install git+https://github.com/quantifyearth/pyshark"))
 (copy (src "./prepare_species") (dst "/root/"))
 (workdir "/root/")
)
```

## Fetching required data

To calculate the AoH we need various basemaps:

* A habitat map, which contains the habitat per pixel
* The Digital Elevation Map (DEM) which has the height per pixel in meters

Both these maps must be at the same pixel spacing and projection, and the output AoH maps will be at that same pixel resolution and projection.

Habitat maps store habitat types in int types typically, the IUCN range data for species are of the form 'x.y' or 'x.y.z', and so you will need to also get a crosswalk table that maps between the IUCN ranges for the species and the particular habitat map you are using.

Here we present the steps required to fetch the [Lumbierres](https://zenodo.org/records/6904020) base maps.

### Fetching the habitat map

To assist with provenance, we download the data from the Zenodo ID.

```shark-run:reclaimer
reclaimer zenodo --zenodo_id 3939050 --filename PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif --output /data/habitat/raw.tif
```

The habitat map by Lumbierres et al is at 100m resolution in World Berhman projection, and for IUCN AoH maps we use Molleide at 1KM resolution. Also, whilst for terrestrial species we use a single habitat map, for other domains we take a map per layer, so this script takes in the original map, splits, reprojects, and rescales it ready for use.


```shark-build:aohbuilderp
((from ghcr.io/osgeo/gdal:ubuntu-small-3.9.2)
(run (network host) (shell "apt-get update -qqy && apt-get -y install python3-pip libpq-dev git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (shell "python3 -m pip config set global.break-system-packages true"))
 (run (network host) (shell "pip install 'numpy<2'"))
 (run (network host) (shell "pip install gdal[numpy]==3.9.2"))
 (copy (src "./aoh-calculator") (dst "/root/"))
 (copy (src "./prepare_layers") (dst "/root/"))
 (workdir "/root/")
 (run (network host) (shell "pip install --no-cache-dir -r ./aoh-calculator/requirements.txt"))
)
```

```shark-run:aohbuilderp
python3 ./aoh-calculator/habitat_process.py --habitat /data/habitat/raw.tif \
                                            --scale 1000.0 \
                                            --projection "ESRI:54009" \
                                            --output /data/habitat_layers/current/

python3 ./prepare_layers/make_masks.py --habitat_layers /data/habitat_layers/current/ \
                                       --output_directory /data/masks/
```



### Fetching the elevation map

To assist with provenance, we download the data from the Zenodo ID.

```shark-run:reclaimer
curl -o FABDEM.zip https://data.bris.ac.uk/datasets/tar/s5hqmjcdj8yo2ibzi9b4ew3sn.zip
...
```

Similarly to the habitat map we need to resample to 1km, however rather than picking the mean elevation, we select both the min and max elevation for each pixel, and then check whether the species is in that range when we calculate AoH.

```shark-run:gdalonly
gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 /data/elevation.tif /data/elevation-min-1k.tif
gdalwarp -t_srs ESRI:54009 -tr 1000 -1000 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 /data/elevation.tif /data/elevation-max-1k.tif
```

### Fetching the species ranges

In this workflow we assume you have a PostGIS database set up with a clone of the IUCN redlist API data already in it, so there is nothing to do here.

## Calculating AoH

Once all the data has been collected, we can now calclate the AoH maps.

### Get per species range data

Rather than calculate from the postgis database directly, we first split out the data into a single GeoJSON file per species per season:

```shark-run:postgis
export DB_HOST=somehost
export DB_USER=username
export DB_PASSWORD=secretpassword
export DB_NAME=iucnredlist

python3 ./prepare-species/extract_species_data_psql.py --class AVES --output /data/species-info/ --projection "ESRI:54009"
```

The reason for doing this primarly one of pipeline optimisation, though it also makes the tasks of debugging and provenance tracing much easier. Most build systems, including the one we use, let you notice when files have updated and only do the work required based on that update. If we have many thousands of species on the redlise and only a few update, if we base our calculation on a single file with all species in, we'll have to calculate all thousands of results. But with this step added in, we will re-generate the per species per season GeoJSON files, which is cheap, but then we can spot that most of them haven't changed and we don't need to then calculate the rasters for those ones in the next stage.

```shark-publish
/data/species-info/
```

### Processing the crosswalk table

The provided crosswalk, derived from Figure 2 in [Lumbierres et al 2021](https://conbio.onlinelibrary.wiley.com/doi/10.1111/cobi.13851), needs first converted to a canonical format used by the software that maps IUCN habitat class to code in habitat raster:

```shark-run:aohbuilder
python3 ./prepare-layers/convert_crosswalk.py --original /data/crosswalk.csv --output /data/processed-crosswalk.csv
```

### Calculate AoH

This step generates a single AoH raster for a single one of the above GeoJSON files.

```shark-run:aohbuilder
python3 ./aoh-calculator/aohcalc.py --habitats /data/habitat_layers/current/ \
                                    --elevation-max /data/elevation-max-1k.tif \
                                    --elevation-min /data/elevation-min-1k.tif \
                                    --crosswalk /data/processed-crosswalk.csv \
                                    --speciesdata /data/species-info/* \
                                    --areas /data/masks/terrestrial_mask.tif \
                                    --output /data/aohs/
```

The results you then want will all be in:

```shark-publish
/data/aohs/
```


## Summaries

Calculate predictors from AoHs


```shark-run:aohbuilder
python3 ./aoh-calculator/summaries/species_richness.py --aohs_folder /data/aohs/current/ \
                                                       --output /data/summaries/species_richness.tif
python3 ./aoh-calculator/summaries/endemism.py --aohs_folder /data/aohs/current/ \
                                               --species_richness /data/summaries/species_richness.tif \
                                               --output /data/summaries/endemism.tif
```

```shark-publish
/data/summaries/species_richness.tif
/data/summaries/endemism.tif
```

## Validation

```shark-run:aohbuilder
python3 ./aoh-calculator/validation/collate_data.py --aohs /data/aohs/ --output /data/validation/aohs.csv
python3 ./aoh-calculator/validation/validate_map_prevelence.py --collated_aoh_data /data/validation/aohs.csv --output /data/validation/model_validation.csv
```

```shark-publish
/data/validation/model_validation.csv
```