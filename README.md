# STAR

An implementation of the threat based [STAR biodiversity metric by Muir et al](https://www.nature.com/articles/s41559-021-01432-0) (also known as STAR(t)).

See [method.md](method.md) for a description of the methodology, or `scripts/run.sh` for how to execute the pipeline.

# Running the pipeline

## Checking out the code

This repository uses submodules, so once you have cloned it, you need to fetch the submodules:

```shell
$ git clone https://github.com/quantifyearth/star.git
$ cd star
$ git submodule update --init --recursive
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
