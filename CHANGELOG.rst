==========
Changelog
==========

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
