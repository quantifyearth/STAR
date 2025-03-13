# STAR

A work in progress implementation of the [STAR biodiversity metric by Muir et al](https://www.nature.com/articles/s41559-021-01432-0). Currently only does the initial AoH stages.

See [method.md](method.md) for a description of the methodology, or `scripts/run.sh` for how to execute the pipeline.


# Running the pipeline

The easiest way to get started will be to run `scripts/run.sh` under a linux environment.

## Running on Ubuntu

The following extra utilities will need to be installed:

* [Reclaimer](https://github.com/quantifyearth/reclaimer/) - a utility for downloading data from various primary sources.

## Running in Docker

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
