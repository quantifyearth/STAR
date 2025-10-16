# STAR

An implementation of the threat based [STAR biodiversity metric by Muir et al](https://www.nature.com/articles/s41559-021-01432-0) (also known as STAR(t)).

See [method.md](method.md) for a description of the methodology, or `scripts/run.sh` for how to execute the pipeline.

# Running the pipeline

## Requirements

The easiest way to run the pipeline is using the included Dockerfile to build a Docker container which will have all the dependancies installed in it.

If not, you will need:

* Python3 >= 3.10
* GDAL
* R (required for validation)
* [Reclaimer](https://github.com/quantifyearth/reclaimer/) - a Go tool for fetching data from Zenodo
* [Littlejohn](https://github.com/quantifyearth/littlejohn/) - a Go tool for running scripts in parallel

If you are using macOS please note that the default Python install that Apple ships is now several years out of date (Python 3.9, released Oct 2020) and you'll need to install a more recent version (for example, using [homebrew](https://brew.sh)).

With those you should set up a Python virtual environment to install all the required packages. The one trick to this is you need to match the Python GDAL package to your installed GDAL version.

```shell
$ python3 -m venv ./venv
$ . ./venv/bin/activate
(venv) $ gdalinfo --version
GDAL 3.11.3 "Eganville", released 2025/07/12
(venv) $ pip install gdal[numpy]==3.11.3
...
(venv) $ pip install -r requirements.txt
```

You will also need to install the R stats packages required for the validation stage:

```shell
$ R -e "install.packages(c('lme4', 'lmerTest'), repos='https://cran.rstudio.com/')"
```

## Additional inputs

There are some additional inputs required to run the pipeline, which should be plated in the directory you use to store the pipeline results.

* crosswalk_bin_T.csv - the crosswalk table from the [Lumbierres et al 2021](https://conbio.onlinelibrary.wiley.com/doi/10.1111/cobi.13851)
* SpeciesList_generalisedRangePolygons.csv - A list of species with generalised ranges on the IUCN Redlist.
* BL_Species_Elevations_2023.csv (optional) - corrections to the elevation of birdlife species on the IUCN Redlist taken from the BirdLife data.

## Running the pipeline

The easiest way to get started will be to run `scripts/run.sh` under a linux environment.

### Running on Ubuntu

The following extra utilities will need to be installed:

* [Reclaimer](https://github.com/quantifyearth/reclaimer/) - a utility for downloading data from various primary sources.
* [Littlejohn](https://github.com/quantifyearth/littlejohn/) - a utility to run jobs in parallel driven by a CSV file.

### Running in Docker

There is included a docker file, which is based on the GDAL container image, which is set up to install everything ready to use. You can build that using:

```
$ docker buildx build -t star .
```

You can then invoke the run script using this. You should map an external folder into the container as a place to store the intermediary data and final results, and you should provide details about the Postgres instance with the IUCN redlist:

```
$ docker run --rm -v /some/local/dir:/data \
	-e DB_HOST=localhost \
	-e DB_NAME=iucnredlist \
	-e DB_PASSWORD=supersecretpassword \
	-e DB_USER=postgres \
	star ./scripts/run.sh
```

# Credits

The author of this package is greatly indebted to both [Francesca Ridley](https://www.ncl.ac.uk/nes/people/profile/francescaridley.html) from the University of Newcastle and [Simon Tarr](https://www.linkedin.com/in/simon-tarr-22069b209/) of the IUCN for their guidance and review.
