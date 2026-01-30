# STAR

An implementation of the threat based [STAR biodiversity metric by Muir et al](https://www.nature.com/articles/s41559-021-01432-0) (also known as STAR(t)).

See [method.md](method.md) for a description of the methodology, or `scripts/run.sh` for how to execute the pipeline.

## Checking out the code

The code is available on github, and can be checked out from there:

```shell
$ git clone https://github.com/quantifyearth/STAR.git
...
$ cd STAR
```

## Additional inputs

There are some additional inputs required to run the pipeline, which should be placed in the directory you use to store the pipeline results.

* SpeciesList_generalisedRangePolygons.csv - A list of species with generalised ranges on the IUCN Redlist.
* BL_Species_Elevations_2023.csv (optional) - corrections to the elevation of birdlife species on the IUCN Redlist taken from the BirdLife data.

The script also assumes you have a Postgres database with the IUCN Redlist database in it.


## Species data acquisition

There are two scripts for getting the species data from the Redlist. For those in the IUCN with access to the database version of the redlist, use `extract_species_data_psql.py`.

For those outside the IUCN, there is a script called `extract_species_data_redlist.py` that gets the data via the [V4 Redlist API](https://api.iucnredlist.org). You will need an API key, which  you can request via the API website [by signing up](https://api.iucnredlist.org/users/sign_up). Once you have that, you still still need to download the ranges for that taxa your interested, as those are not available from the API, so before running the script you must [go to the spacial data portal](https://www.iucnredlist.org/resources/spatial-data-download) and download the files for the TAXA you are interested in.

## Running the pipeline

There are two ways to run the pipeline. The easiest way is to use Docker if you have it available to you, as it will manage all the dependencies for you. But you can check out and run it locally if you want to also, but it requires a little more effort.

Either way, the pipeline itself is ran using [Snakemake](https://snakemake.readthedocs.io/en/stable/), which is a tool designed to run data-science pipelines made up from many different scripts and sources of information. Snakemake will track dependancies making it easier to re-run the pipeline and only the bits that depend on what changed will rerun. However, in STAR the initial data processing of raster layers is very slow, so we've configured Snakemake to never re-generate those unless the generated rasters have been deleted manually.

Because sometimes you do not need to run all the pipeline for a specific job, the snakemake script has multiple targets you can invoke:

* prepaer: Generate the necessary input rasters for the STAR pipeline.
* species_data: Extract species data into GeoJSON files from Redlist database.
* aohs: Just generate the species AOHs and summary CSV.
* validation: Run model validation.
* occurrence_validation: Run occurrence validation - this can be VERY SLOW as it fetches occurrence data from GBIF.
* threats: Generate the STAR(t) raster layers.
* all: Do everything except occurrence validation.

### Running with Docker

There is included a docker file, which is based on the GDAL container image, which is set up to install everything ready to use. You can build that using:

```shell
$ docker buildx build -t star .
```

Note that depending on how many CPU cores you provide, you will probably need to give Docker more memory that the out of the box setting (which is a few GB). We recommend giving it as much as you can allow.

You can then invoke the run script using this. You should map an external folder into the container as a place to store the intermediary data and final results, and you should provide details about the Postgres instance with the IUCN redlist:

```shell
$ docker run --rm -v /some/local/dir:/data \
	-p 5432:5432 \
	-e DB_HOST=localhost \
	-e DB_NAME=iucnredlist \
	-e DB_PASSWORD=supersecretpassword \
	-e DB_USER=postgres \
	-e GBIF_USERNAME=myusename \
	-e GBIF_PASSWORD=mypassword \
	-e GBIF_EMAIL=myemail \
	star --cores 8 all
```

### Running without Docker

If you prefer not to use Docker, you will need:

* Python3 >= 3.10
* GDAL
* R (required for validation)
* [Reclaimer](https://github.com/quantifyearth/reclaimer/) - a Go tool for fetching data from Zenodo

If you are using macOS please note that the default Python install that Apple ships is now several years out of date (Python 3.9, released Oct 2020) and you'll need to install a more recent version (for example, using [homebrew](https://brew.sh)).

With those you should set up a Python virtual environment to install all the required packages. The one trick to this is you need to match the Python GDAL package to your installed GDAL version. For example, on my machine I did the following:

```shell
$ python3 -m venv ./venv
$ . ./venv/bin/activate
(venv) $ pip install gdal[numpy]==`gdal-config --version`
...
(venv) $ pip install -r requirements.txt
```

You will also need to install the R stats packages required for the validation stage:

```shell
$ R -e "install.packages(c('lme4', 'lmerTest'), repos='https://cran.rstudio.com/')"
```

Before running the pipeline you will need to set several environmental variables to tell the script where to store data and where the database with the IUCN Redlist is. You can set these manually, or we recommend using a tool like [direnv](https://direnv.net).

```shell
export DATADIR=[PATH WHERE YOU WANT THE RESULTS]
export DB_HOST=localhost
export DB_NAME=iucnredlist
export DB_PASSWORD=supersecretpassword
export DB_USER=postgres
```

If on macOS then you can set the following extra flag to use GPU acceleration:

```shell
export YIRGACHEFFE_BACKEND=MLX
```

For occurrence validation you will need a GBIF account and have to set the details as follows:

```shell
export GBIF_USERNAME=myusename
export GBIF_PASSWORD=mypassword
export GBIF_EMAIL=myemail
```

Once you have all that you can then run the pipeline:

```shell
(venv) $ ./scripts/run.sh
```

# Credits

The author of this package is greatly indebted to both [Francesca Ridley](https://www.ncl.ac.uk/nes/people/profile/francescaridley.html) from the University of Newcastle and [Simon Tarr](https://www.linkedin.com/in/simon-tarr-22069b209/) of the IUCN for their guidance and review.

## Data Attribution

The crosswalk table `data/crosswalk_bin_T.csv` was created by [Francesca Ridley](https://www.ncl.ac.uk/nes/people/profile/francescaridley.html) and is derived from:

```
Lumbierres, M., Dahal, P.R., Di Marco, M., Butchart, S.H.M., Donald, P.F.,
& Rondinini, C. (2022). Translating habitat class to land cover to map area
of habitat of terrestrial vertebrates. Conservation Biology, 36, e13851.
https://doi.org/10.1111/cobi.13851
```

The paper is licensed under CC BY-NC. It is used in this STAR implementation to crosswalk between the IUCN Habitat classes in the Redlist and the land classes in the Copernicus data layers.

