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

Requirements
------------

+ PostgreSQL

    + I'm testing against Postgres 10 + PostGIS 2.5, Postgres 15 + PostGIS 3.3


Install
-------

The software does not have an installer but only an executable which you can run directly in the command line. Head over to the `latest release <https://github.com/cityjson/cjio_dbexport/releases/latest>`_ and download the executable for your platform.

Extract the archive (in Windows you'll need 7zip) and run the executable from the command line.

Install for development
-----------------------

Requires Python 3.6+

The project is alpha, please install directly from GitHub with pip:

.. code-block::

    $ pip install -U --force-reinstall git+https://github.com/cityjson/cjio_dbexport@master

Also install the development requirements from ``requirements_dev.txt``

Usage
-----

Call the *cjdb* tool from the command line and pass it the configuration file.

.. code-block::

    $ cjdb --help

    Usage: cjdb [OPTIONS] CONFIGURATION COMMAND [ARGS]...

      Export tool from PostGIS to CityJSON.

      CONFIGURATION is the YAML configuration file.

    Options:
      --version                       Show the version and exit.
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

* The block ``geometries`` declares the Level of Detail (LoD) in and geometry type in of the CityObjects. The geometry type is one of the allowed `CityJSON geometry types <https://www.cityjson.org/specs/1.0.1/#arrays-to-represent-boundaries>`_. The LoD can be either an integer (following the CityGML standards), or a number following the `improved LoDs by TU Delft <https://3d.bk.tudelft.nl/lod/>`_.

* The block ``database`` specifies the database connection parameters. The password can be empty if it is stored a in a ``.pgpass`` file.

* The block ``tile_index`` specifies the location of the *tile index* for using with the ``export_tiles`` command.

* The block ``cityobject_type`` maps the database tables to CityObject types.

Each key in ``cityobject_type`` is a `1st-level or 2nd-level CityObject <https://www.cityjson.org/specs/1.0.1/#city-object>`_ , and it contains a sequence of mappings. Each of these mappings refer to a single table, thus you can collect CityObjects from several tables into a single CityObject type. The ``table`` is exported as **one record per CityObject**.

The mapping of the fields of the table to CityObjects is done as:

+ ``pk``: the primary key
+ ``geometry``: the geometry field
+ ``cityobject_id``: the field that is used as CityObject ID

By default all columns, excluding the three above, are added as Attributes to the CityObject. If you want to exclude certain fields, specify the filed names in a string array in ``exclude``. In the example below, the fields ``xml`` and ``_clipped`` are excluded from the export.

.. code-block::

    geometries:
      lod: 1.2
      type: MultiSurface

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
              lod12: 
                name: wkb_geometry
                type: Solid
              lod13: 
                name: wkb_geometry_lod13
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

    $ cjdb config.yml export_bbox 123.4 545.5 678.8 987.8 path/to/output.json

To export an irregular extent, provide a single Polygon in a GeoJSON file.

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
            lod0: 
              name: geom_lod0
            lod13: 
              name: geom_lod13

Notice that,

* ``geometry`` becomes a mapping of mappings,

* the keys in ``geometry`` follow the convention of ``lod<value>``, where ``<value>`` is the level of detail,

* the ``lod<value>`` keys are mappings of the geometry column with the corresponding LoD, where the ``name`` key points to the name of the column.

For example if you want to export the LoD0 and LoD1.3 (see yaml above) but write each LoD into a separate file, 
then you need to run the export process twice. Once for each LoD, 
by keeping only ``lod0.name.geom_lod0`` or ``lod13.name.geom_lod13`` respectively for the 
desired LoD.

Global and per-table LoD and geometry type settings
***************************************************

The global (for the whole file) setting for both the LoD and the output
geometry type is in the ``geometries`` block in the configuration file. By default each geometry will get the global LoD and geometry type on export.

If you want to export a table to a different geometry type than that of the global setting, then you need to declare it under the corresponding LoD-key in the ``geometry`` mapping of the table.


.. code-block::

  geometries:
    lod: 0
    type: MultiSurface

  cityobject_type:
    Building:
      - schema: public
        table: building
        field:
          pk: ogc_fid
          geometry:
            lod0: 
              name: geom_lod0
            lod13: 
              name: geom_lod13
              type: Solid

Furthermore, it is possible to assign different a different LoD per object. In this case the LoD name (e.g. ``1.2`` or ``2``) is expected to be stored in column of the CityObject table. In the example below each CityObject will get the LoD that is stored in the ``lod_column`` column of the ``building`` table.

.. code-block::

  cityobject_type:
    Building:
      - schema: public
        table: building
        field:
          pk: ogc_fid
          geometry: geometry_column
          lod: lod_column

When the LoD is declared on multiple levels for a CityObject (e.g. on global, per column or per object) then the lower-level declaration overrules the higher-level one. For instance the per-column declaration overrules the global.


Exporting semantic surfaces
***************************

In order to export LoD2 models with semantic surfaces, the semantics need to be in the same table as the geometry. The semantics need to be stored as an array of integers where,

1. the array has the same length and order as the surfaces of the geometry,
2. the integers in the array represent the semantics.

This is almost the same as the ``values`` member of a `Semantic Surface object <https://www.cityjson.org/specs/1.0.1/#semantic-surface-object>`_, with some distinctions,

+ Null values are not supported in the array,
+ there is no distinction in the array-nesting for the different geometry types.

An example of such a semantics array would be implemented as ``'{0,0,2,2,2,2,2,2,2,2,1,1}'::int2[]`` in PostgreSQL.

Note that the semantics array above is 12 elements long, thus the first element points to the first surface of the geometry boundary (the MultiPolygonZ), while the 12th element points to the 12th surface of the geometry boundary.

The integer mapping of the semantic values are declared at the root level of the  configuration file.

Additionally, in the ``semantics`` member of the table fields you need to declare the name of the column in which the semantic arrays are stored.

It is not possible to export multiple LoDs into the same CityJSON file when exporting semantics too.

.. code-block::

    semantics_mapping:
      0: "GroundSurface"
      1: "RoofSurface"
      2: "WallSurface"

    cityobject_type:
      Building:
        - schema: public
          table: lod2_sample
          field:
            pk: ogc_fid
            geometry:
              lod22:
                name: wkb_geometry
                type: Solid
            semantics: labels
            cityobject_id: id_column


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

+ No appearances

+ The geometry is expected to be a ``MULTIPOLYGON`` of ``POLYGON Z`` in PostGIS

+ CRS is hardcoded to 7415


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
