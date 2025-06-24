==========
Changelog
==========

0.9.3 (2025-06-24)
------------------

Changes
*******
* Update to psycopg3 and update all other dependencies
* Replace the db module with pgutils
* Migrate to pyproject.toml setup
* Testing with postgres 17, python 12

0.9.2 (2023-06-21)
------------------

Changes
*******
* Lower the scale factor from 0.001 to 0.0001 in the Netherlands transform, because too low precision might move the coordinates in a way that leads to errors, like spike in triangulation later on.
* Copy tile-by-tile when inserting the tile index, instead of bulk copy of the whole index (#35).

Adds
****
* Netherlands geojson example (#36).
* GitHub Actions release workflow.


0.9.0 (2023-02-06)
------------------

Changes
*******
* Update to CityJSON 1.1 (replaces the CityJSON 1.0 export)
* Upgrade to cjio 0.8.0
* Update Kadaster test data
* Test against Postgres 15 + PostGIS 3.3 (instead of 13+3.0)

Adds
****
* Export CityJSONFeatures (only with the `export_tiles` command). The Transform parameters are hardcoded to the Zwaartepunt bij Putten in the Netherlands.
* Create GIST index on the feature geometry centroids with `index --centroid`. This is required for exporting CityJSONFeatures.
* Optional file name prefix

Removes
*******
* The 'tile_id' attribute from the output

0.8.4 (2021-01-25)
------------------

Fixes
*****
* LoD is a float now, as it is required by the specs
* Attribute excludes were ignored

0.8.0 (2020-10-10)
------------------

Fixes
*****
* Do not require a global geometry type and lod declaration
* LoD parsing when lod is declared globally and using the improved LoD-s

Adds
****
* Semantic surface export

0.7.0 (2020-10-09)
------------------

Fixes
*****
* Incorrect 'tablename' was passed down within query()
* Travis-CI for the Ubunutu exe

Adds
****
* Complete type conversion for the `date`, `time/tz`, `timestamp/tz`, `interval` postgres types
* Docker setup for testing
* Possible to set the LoD per feature if the LoD name is stored in a column
* Extended CityJSON metadata
* Update the exe-s to Python 3.8

0.6.1 (2020-07-15)
-------------------

Adds
*****
* Compression to `export_tiles`
* Remove all whitespace characters

0.6.0 (2020-05-06)
-------------------

Breaking changes
****************
* the ``geometries`` mapping is required in the YAML configuration file
* Creates the ``cjdb_multipolygon_to_multisurface(geometry)`` SQL function, thus requires a user with CREATE FUNCTION permission

Adds
*****
* Possible to export multiple LoD when the different LoDs are stored in the same table
* Possible to set the output geometry type per table

Fixes
*****
* Fix surface orientation (#20)
* Fix ``--version`` in the exe

0.1.0 (2019-12-16)
------------------

* Testing the export of Buildings from the 3DNL database
