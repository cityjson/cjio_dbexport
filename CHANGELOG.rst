==========
Changelog
==========

0.6.0
-----

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

0.1.0 (2019-12-16)
------------------

* Testing the export of Buildings from the 3DNL database