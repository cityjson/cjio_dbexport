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

Requires Python 3.5+

The project is pre-alpha, please install directly from GitHub with pip:

.. code-block::

    $ pip install -U --force-reinstall git+https://github.com/balazsdukai/cjio_dbexport@master

Usage
-----

Call the *cjio_dbexport* tool from the command line and pass it the configuration file.

.. code-block::

    $ cjio_dbexport tests/data/test_config.yml --help

    Usage: cjio_dbexport [OPTIONS] CONFIGURATION COMMAND [ARGS]...

      Export tool from PostGIS to CityJSON.

      CONFIGURATION is the YAML configuration file.

    Options:
      -v, --verbose  Increase verbosity. You can increment the level by chaining
                     the argument, eg. -vvv
      -q, --quiet    Decrease verbosity.
      --help         Show this message and exit.

    Commands:
      export  Export into a CityJSON file.


This tool uses a YAML-based configuration file to managing the database connections and declaring what to export. The block ``database`` specifies the database connection parameters. The block ``features`` specifies which table and fields to export from the database. The ``table`` is exported as **one record per CityObject**.

The mapping of the fields of the table to CityObjects is done as:

+ ``pk``: the primary key
+ ``geometry``: the geometry field
+ ``cityobject_id``: the field that is used as CityObject ID

By default all columns, excluding the three above, are added as Attributes to the CityObject. If you want to exclude certain fields, specify the filed names in a string array in ``exclude``. In the example below, the fields ``xml`` and ``_clipped`` are excluded from the export.

.. code-block::

    database:
        dbname: db3dnl
        host: localhost
        port: 5432
        user: some_user
        password: some_password

    features:
        schema: public
        table: building
        field:
            pk: ogc_fid
            geometry: wkb_geometry
            cityobject_id: identificatie
            exclude: ["xml", "_clipped"]

You can provide a bounding box (minx miny maxx maxy) to limit the extent of the export.

.. code-block::

    $ cjio_dbexport tests/data/test_config.yml export --bbox 123.4 545.5 678.8 987.8 path/to/some/file.json


Limitations
------------

+ Only works for the *Building* CityObject type, hardcoded to LoD 1, no semantics, no appearances

+ The geometry is expected to be a ``MULTIPOLYGON`` of ``POLYGON Z`` in PostGIS

+ Either export the whole table of ``features.table``, or subset with a bounding box

+ Only tested with Python 3.6, PostgresSQL 11, PostGIS 2.5

+ CRS is hardcoded to 7415


Features (planned)
------------------

+ [x] CLI

+ [ ] export a single irregular extent, provided by a polygon in geojson

+ [ ] export multiple tiles, provided by tile IDs

+ [ ] export all tiles

+ [ ] concurrent export of the tiles


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
