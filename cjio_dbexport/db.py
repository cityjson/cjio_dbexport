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

import psycopg2
from psycopg2 import sql, extras, extensions

log = logging.getLogger(__name__)


class Db(object):
    """A database connection class.

    :raise: :class:`psycopg2.OperationalError`
    """

    def __init__(self, dbname, host, port, user, password=None):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        try:
            self.conn = psycopg2.connect(
                dbname=dbname, host=host, port=port, user=user,
                password=password
            )
            log.debug(f"Opened connection to {self.conn.get_dsn_parameters()}")
        except psycopg2.OperationalError:
            log.exception("I'm unable to connect to the database")
            raise

    def send_query(self, query: psycopg2.sql.Composable):
        """Send a query to the DB when no results need to return (e.g. CREATE).
        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)

    def get_query(self, query: psycopg2.sql.Composable) -> List[Tuple]:
        """DB query where the results need to return (e.g. SELECT)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_dict(self, query: psycopg2.sql.Composable) -> dict:
        """DB query where the results need to return as a dictionary."""
        with self.conn:
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def print_query(self, query: psycopg2.sql.Composable) -> str:
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
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        schema = psycopg2.sql.Identifier(schema)
        table = psycopg2.sql.Identifier(table)
        query = psycopg2.sql.SQL("""
        VACUUM ANALYZE {schema}.{table};
        """).format(schema=schema, table=table)
        self.send_query(query)

    def vacuum_full(self):
        """Vacuum analyze the whole database."""
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        query = psycopg2.sql.SQL("VACUUM ANALYZE;")
        self.send_query(query)

    def check_postgis(self):
        """Create the PostGIS extension if not exists."""
        self.send_query("CREATE EXTENSION IF NOT EXISTS postgis;")

    def get_fields(self, schema, table):
        """List the fields in a table."""
        query = sql.SQL("SELECT * FROM {s}.{t} LIMIT 0;").format(
            s=sql.Identifier(schema), t=sql.Identifier(table))
        cols = self.get_query(query)
        yield [c[0] for c in cols]

    def close(self):
        """Close connection."""
        self.conn.close()
        log.debug("Closed database successfully")


def identifier(relation_name):
    """Property factory for returning a :class:`psycopg2.sql.Identifier`."""
    def id_getter(instance):
        return sql.Identifier(instance.__dict__[relation_name])

    def id_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(id_getter, id_setter)


def literal(relation_name):
    """Property factory for returning a :class:`psycopg2.sql.Literal`."""
    def lit_getter(instance):
        return sql.Literal(instance.__dict__[relation_name])

    def lit_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(lit_getter, lit_setter)


class DbRelation:
    """Database relation name.

    An escaped SQL identifier of the relation name is accessible through the
    `sqlid` property, which returns a :class:`psycopg2.sql.Identifier`.

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
    >>> index.schema.name
    'tile_index'
    >>> index.schema.identifier
    Identifier('tile_index')
    >>> index.schema + index.table
    Identifier('tile_index', 'bag_index_test')
    """

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

