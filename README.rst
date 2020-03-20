=============
cjio_dbexport
=============

..
    .. image:: https://img.shields.io/travis/balazsdukai/cjio_dbexport.svg
            :target: https://travis-ci.org/balazsdukai/cjio_dbexport

    .. image:: https://readthedocs.org/projects/cjio-dbexport/badge/?version=latest
            :target: https://cjio-dbexport.readthedocs.io/en/latest/?badge=latest
            :alt: Documentation Status



Export tool from PostGIS to CityJSON


License
-------

MIT license

..
    * Documentation: https://cjio-dbexport.readthedocs.io.

Install
-------

The software does not have an installer but only an executable which you can
run directly in the command line. Head over to the `latest release
<https://github.com/cityjson/cjio_dbexport/releases/latest>`_ and download
the executable for your platform.

Extract the archive (in Windows you'll need 7zip) and run the executable from
the command line.

Install for development
-----------------------

Requires Python 3.6+

The project is alpha, please install directly from GitHub with pip:

.. code-block::

    $ pip install -U --force-reinstall git+https://github.com/cityjson/cjio_dbexport@master

Usage
-----

Call the *cjdb* tool from the command line and pass it the configuration file.

.. code-block::

    $ cjdb --help

    Usage: cjdb [OPTIONS] CONFIGURATION COMMAND [ARGS]...

      Export tool from PostGIS to CityJSON.

      CONFIGURATION is the YAML configuration file.

    Options:
      --log [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                      Set the logging level in the log file
                                      'cjdb.log'.
      --help                          Show this message and exit.

    Commands:
      export         Export the whole database into a CityJSON file.
      export_bbox    Export the objects within a 2D Bounding Box into a
                     CityJSON...
      export_extent  Export the objects within the given polygon into a
                     CityJSON...
      export_tiles   Export the objects within the given tiles into a CityJSON...
      index          Create a tile index for the specified extent.


This tool uses a YAML-based configuration file to managing the database
connections and to declare what to export.

* The block ``lod`` declares the Level of Detail of the CityObjects.

* The block ``database`` specifies the database connection parameters.

* The block ``cityobject_type`` maps the database tables to CityObject types.

* Each key in ``cityobject_type`` is a `1st-level or 2nd-level CityObject<https://www.cityjson.org/specs/1.0.1/#city-object>`_, and it contains a sequence of mappings. Each of these mappings refer to a single table, thus you can collect CityObjects from several tables into a single CityObject type. The ``table`` is exported as **one record per CityObject**.

The mapping of the fields of the table to CityObjects is done as:

+ ``pk``: the primary key
+ ``geometry``: the geometry field
+ ``cityobject_id``: the field that is used as CityObject ID

By default all columns, excluding the three above, are added as Attributes to the CityObject. If you want to exclude certain fields, specify the filed names in a string array in ``exclude``. In the example below, the fields ``xml`` and ``_clipped`` are excluded from the export.

.. code-block::

    lod: 1.2

    database:
      dbname: db3dnl
      host: localhost
      port: 5432
      user: some_user
      password: some_password

    tile_index:
      schema: tile_index
      table: tile_index_1
      srid: 7415
      field:
        pk: id
        geometry: geom

    cityobject_type:
      Building:
        - schema: public
          table: building
          field:
            pk: ogc_fid
            geometry:
              - lod12: wkb_geometry
              - lod13: wkb_geometry_lod13
            cityobject_id: identificatie
            exclude: ["xml", "_clipped"]
      WaterBody:
        - schema: public
          table: waterdeel_vlak
          field:
            pk: ogc_fid
            geometry: wkb_geometry
            cityobject_id: identificatie
            exclude: ["xml", "_clipped"]
      LandUse:
        - schema: public
          table: onbegroeidterreindeel_vlak
          field:
            pk: ogc_fid
            geometry: wkb_geometry
            cityobject_id: identificatie
            exclude: ["xml"]
        - schema: public
          table: ondersteunendwaterdeel_vlak
          field:
            pk: ogc_fid
            geometry: wkb_geometry
            cityobject_id: identificatie
            exclude: ["xml"]

Exporting a subset
******************

You can provide a bounding box (minx miny maxx maxy) to limit the extent of the export.

.. code-block::

    $ cjdb config.yml export_bbox 123.4 545.5 678.8 987.8
path/to/output.json

To export an irregular extent, provide a single
Polygon in a GeoJSON file.

.. code-block::

    $ cjdb config.yml export_extent polygon.geojson path/to/output.json

To export a set of tiles into a separate CityJSON file each, provide their
tile IDs. The command below will export the tiles ``ci1``, ``ci2``, ``gb4``
into the given directory. If you want to merge the tiles into a single file,
provide
the ``--merge`` option to ``export_tiles``. If you want to export all the
tiles from the *tile index*, then pass ``all`` as the tile ID.

.. code-block::

    $ cjdb config.yml export_tiles ci1 ci2 gb4 path/to/directory

Exporting citymodels in multiple Level of Detail (LoD)
******************************************************

The ``lod`` parameter in the YAML configuration file declares the LoD value 
that each CityObject will get in the output file. However, in case you have 
objects with multiple geometric representations (multiple LoD), you can 
choose to export the each LoD into the same file or write a separate file 
for each LoD.

For instance we have a table that stores building models and each building 
has a geometry in LoD0 and LoD1.3. Note that this is the case of single 
table with multiple geometry columns. In this case we can declare the 
mapping of the geometry column as here below.

.. code-block::

  cityobject_type:
    Building:
      - schema: public
        table: building
        field:
          pk: ogc_fid
          geometry:
            - lod0: geom_lod0
            - lod13: geom_lod13

Notice that,

* ``geometry`` becomes an array of key-value pairs instead of a single key-value pair,

* the keys in ``geometry`` follow the convention of ``lod<value>``, where ``<value>`` is the level of detail,

* the ``lod<value>`` keys point to the geometry column with the corresponding LoD

By default each LoD is written to a separate file. by using the ``--multi-lod`` 
command line option it is possible to write all the LoDs to a single file.


Creating a tile index
*********************

If you have a database of a large area, you probably want to export it
piece-by-piece, in tiles. This requires a *tile index*, which is a rectangular
grid of polygons that fully covers your area, and each polygon has a unique ID.

The ``index`` command can help you create such a tile index. It requires a
polygonal *extent* of your area as GeoJSON file and the *width* and *height*
of the tiles you want to create. The units for the tile size are same as the
unit of the CRS in the database.

.. code-block::

    $ cjdb config.yml index netherlands.json 1000 1000

The command above will,

1. create rectangular polygons (tiles) of 1000m by 1000m for the extent
of the polygon that is ``netherlands.json``,

2. sort the tiles in Morton-order and create unique IDs for them
accordingly,

3. upload the tile index into the relation that is declared in
``config.yml`` under the ``tile_index`` node.


Limitations
------------

+ Hardcoded to LoD 1, no semantics, no appearances

+ The geometry is expected to be a ``MULTIPOLYGON`` of ``POLYGON Z`` in PostGIS

+ Only tested with PostgresSQL 11, PostGIS 2.5

+ CRS is hardcoded to 7415


Features (planned)
------------------

See `the 3DNL project <https://github.com/cityjson/cjio_dbexport/projects/1>`_


3DNL
-----

Mapping of the 3DNL tables to CityJSON CityObjects:

+-----------------------------+-------------------+
| 3dnl table                  | CityObject type   |
+=============================+===================+
| begroeidterreindeel_vlak    | PlantCover        |
+-----------------------------+-------------------+
| building                    | Building          |
+-----------------------------+-------------------+
| kunstwerkdeel_vlak          | GenericCityObject |
+-----------------------------+-------------------+
| onbegroeidterreindeel_vlak  | LandUse           |
+-----------------------------+-------------------+
| ondersteunendwaterdeel_vlak | LandUse           |
+-----------------------------+-------------------+
| ondersteunendwegdeel_vlak   | LandUse           |
+-----------------------------+-------------------+
| overbruggingsdeel_vlak      | Bridge            |
+-----------------------------+-------------------+
| overigbouwwerk              | LandUse           |
+-----------------------------+-------------------+
| pand                        | LandUse           |
+-----------------------------+-------------------+
| scheiding_vlak              | GenericCityObject |
+-----------------------------+-------------------+
| waterdeel_vlak              | WaterBody         |
+-----------------------------+-------------------+
| wegdeel_vlak                | Road              |
+-----------------------------+-------------------+

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
