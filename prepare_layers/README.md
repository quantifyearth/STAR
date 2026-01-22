# Preparing input rasters for STAR

## Elevation

STAR uses [FABDEM v1.2](https://data.bris.ac.uk/data/dataset/s5hqmjcdj8yo2ibzi9b4ew3sn) for its elevation layer, however this raster needs to be processed before it can be used:

* There are some errata tiles to correct mistakes in the area of Florida's Forgotten Coast. Unfortunately these are only available at time of writing via a [Google Drive link](https://drive.google.com/file/d/1DIAaheKT-thuWPhzXWpfVnTqRn13nqvi/view?usp=sharing).
* FABDEM was created when certain tiles in the Copernicus GLO DEM were not available, leaving a gap around Azerbaijan and nearby countries.

FABDEM itself is very large and slow to download, and so we leave that as an exercise for the user rather than automating it as part of the pipeline. Once that has downloaded and the google drive link has been followed and the tiles expanded, we provide two scripts to complete the job:

* `fetch_cglo.py` - this will fetch the missing tiles from Copernicus GLO that are now available.
* `make_hybrid_elevation_map.py` - this takes three inputs: a folder with the FABDEM v1.2 tiles, a folder with the errata files from Google Drive, and a folder with the additional CGLO tiles, and outputs the compiled hybrid elevation map. Note that this will be around 500 GB in size.