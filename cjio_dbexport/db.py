# -*- coding: utf-8 -*-
"""Database connection handling.

Copyright (c) 2019, 3D geoinformation group, Delft University of Technology.

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import re
from typing import List, Tuple
from collections import abc
from keyword import iskeyword

import psycopg
from psycopg import sql, rows

log = logging.getLogger(__name__)


class Db(object):
    """A database connection class.

    :raise: :class:`psycopg.OperationalError`
    """

    def __init__(self, conn=None, dbname=None, host=None, port=None,
                 user=None, password=None):
        if conn is None:
            self.dbname = dbname
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            try:
                self.conn = psycopg.connect(
                    dbname=dbname, host=host, port=port, user=user,
                    password=password
                )
                log.debug(f"Opened connection to {self.conn.info.get_parameters()}")
            except psycopg.OperationalError:
                log.exception("I'm unable to connect to the database")
                raise
        else:
            self.conn = conn

    def dsn(self):
        """PostgreSQL's connection Data Source Name (DSN)."""
        _d = []
        if self.dbname:
            _d.append(f"dbname={self.dbname}")
        if self.user:
            _d.append(f"user={self.user}")
        if self.port:
            _d.append(f"port={self.port}")
        if self.host:
            _d.append(f"host={self.host}")
        if self.password:
            _d.append(f"password={self.password}")
        return " ".join(_d)

    def send_query(self, query: psycopg.sql.Composable):
        """Send a query to the DB when no results need to return (e.g. CREATE).
        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)

    def get_query(self, query: psycopg.sql.Composable) -> List[Tuple]:
        """DB query where the results need to return (e.g. SELECT)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_dict(self, query: psycopg.sql.Composable) -> dict:
        """DB query where the results need to return as a dictionary."""
        with self.conn:
            with self.conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(query)
                return cur.fetchall()

    def print_query(self, query: psycopg.sql.Composable) -> str:
        """Format a SQL query for printing by replacing newlines and tab-spaces.
        """

        def repl(matchobj):
            if matchobj.group(0) == '    ':
                return ' '
            else:
                return ' '

        s = query.as_string(self.conn).strip()
        return re.sub(r'[\n    ]{1,}', repl, s)

    def vacuum(self, schema: str, table: str):
        """Vacuum analyze a table."""
        # Set autocommit mode for vacuum operations
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True
        try:
            schema = sql.Identifier(schema)
            table = sql.Identifier(table)
            query = sql.SQL("""
            VACUUM ANALYZE {schema}.{table};
            """).format(schema=schema, table=table)
            self.send_query(query)
        finally:
            # Restore original autocommit setting
            self.conn.autocommit = old_autocommit

    def vacuum_full(self):
        """Vacuum analyze the whole database."""
        # Set autocommit mode for vacuum operations
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True
        try:
            query = sql.SQL("VACUUM ANALYZE;")
            self.send_query(query)
        finally:
            # Restore original autocommit setting
            self.conn.autocommit = old_autocommit

    def check_postgis(self):
        """Check if PostGIS is installed."""
        try:
            version = self.get_query("SELECT PostGIS_version();")[0][0]
        except psycopg.Error as e:
            version = None
        return version

    def get_fields(self, table):
        """List the fields in a table."""
        query = sql.SQL("SELECT * FROM {table} LIMIT 0;").format(table=table)
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return [desc.name for desc in cur.description]

    def close(self):
        """Close connection."""
        self.conn.close()
        log.debug("Closed database successfully")

    def create_functions(self) -> bool:
        """Create the required functions in PostgreSQL.

        ``cjdb_multipolygon_to_multisurface()``

            Parse the PostGIS geometry representation into
            a CityJSON-like geometry array representation. Here I use
            several subqueries for sequentially aggregating the vertices,
            rings and surfaces. I also tested the aggregation with window
            function calls, but this approach tends to be at least twice
            as expensive then the subquery-aggregation.
            In the expand_point subquery, the first vertex is skipped,
            because PostGIS uses Simple Features so the first vertex is
            duplicated at the end.
        """
        mpoly_to_msrf = sql.SQL("""
                                CREATE OR REPLACE
                                    FUNCTION cjdb_multipolygon_to_multisurface(
                                    multipolygon geometry
                                ) RETURNS FLOAT8[] AS
                                $$
                                WITH polygons
                                    AS (SELECT ST_DumpPoints(multipolygon) geom)
                                   , expand_points AS (SELECT (geom).PATH[1] exterior
                                                            , (geom).PATH[2] interior
                                                            , (geom).PATH[3] vertex
                                                            , ARRAY [ST_X((geom).geom)
                                        , ST_Y((geom).geom)
                                        , ST_Z((geom).geom)]                 point
                                                       FROM polygons
                                                       WHERE (geom).PATH[3] > 1
                                                       ORDER BY exterior
                                                              , interior
                                                              , vertex)
                                   , rings AS (SELECT exterior
                                                    , interior
                                                    , ARRAY_AGG(point) geom
                                               FROM expand_points
                                               GROUP BY interior
                                                      , exterior
                                               ORDER BY exterior
                                                      , interior)
                                   , surfaces AS (SELECT ARRAY_AGG(geom) geom
                                                  FROM rings
                                                  GROUP BY exterior
                                                  ORDER BY exterior)
                                SELECT ARRAY_AGG(geom) geom
                                FROM surfaces;
                                $$ LANGUAGE SQL;

                                COMMENT ON
                                    FUNCTION cjdb_multipolygon_to_multisurface(
                                    IN multipolygon geometry
                                    ) IS 'Cast a PostGIS MultiPolygon geometry into a CityJSON MultiSurface 
        geometry array representation.';
                                """)
        success = []
        try:
            self.send_query(mpoly_to_msrf)
            log.info("Created PostgreSQL FUNCTION "
                     "cjdb_multipolygon_to_multisurface()")
            success.append(True)
        except psycopg.Error as e:
            log.exception(f"Error creating PostgreSQL FUNCTION "
                          f"cjdb_multipolygon_to_multisurface()\n{e}")
            success.append(False)
        return all(success)


def identifier(relation_name):
    """Property factory for returning a :class:`psycopg.sql.Identifier`."""

    def id_getter(instance):
        return sql.Identifier(instance.__dict__[relation_name])

    def id_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(id_getter, id_setter)


def literal(relation_name):
    """Property factory for returning a :class:`psycopg.sql.Literal`."""

    def lit_getter(instance):
        return sql.Literal(instance.__dict__[relation_name])

    def lit_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(lit_getter, lit_setter)


class DbRelation:
    """Database relation name.

    An escaped SQL identifier of the relation name is accessible through the
    `sqlid` property, which returns a :class:`psycopg.sql.Identifier`.

    Concatenation of identifiers is supported through the `+` operator.
    For example `DbRelation('schema') + DbRelation('table')`.
    """
    sqlid = identifier('sqlid')

    def __init__(self, relation_name):
        self.sqlid = relation_name
        self.string = relation_name

    def __repr__(self):
        return self.string

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return sql.Identifier(self.string, other.string)
        else:
            raise TypeError(f"Unsupported type {other.__class__}")


class Schema:
    """Database relations.

    The class maps a dictionary to object, where the dict keys are accessible
    as object attributes. Additionally, the values (eg. table name) can be
    retrieved as an escaped SQL identifier through the `sqlid` property.

    >>> relations = {
        'schema': 'tile_index',
        'table': 'bag_index_test',
            'fields': {
            'geometry': 'geom',
            'primary_key': 'id',
            'unit_name': 'bladnr'}
        }
    >>> index = Schema(relations)
    >>> index.schema
    'tile_index'
    >>> index.schema.identifier
    Identifier('tile_index')
    >>> index.schema + index.table
    Identifier('tile_index', 'bag_index_test')
    """

    # TODO: skip Lists
    def __new__(cls, arg):
        if isinstance(arg, abc.Mapping):
            return super().__new__(cls)
        elif isinstance(arg, abc.MutableSequence):
            return [cls(item) for item in arg]
        else:
            return DbRelation(arg)

    def __init__(self, mapping):
        self.__data = {}
        for key, value in mapping.items():
            if iskeyword(key):
                key += '_'
            self.__data[key] = value

    def __getattr__(self, name):
        if hasattr(self.__data, name):
            return getattr(self.__data, name)
        else:
            return Schema(self.__data[name])