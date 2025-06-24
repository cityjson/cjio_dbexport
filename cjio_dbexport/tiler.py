# -*- coding: utf-8 -*-
"""Partitioning the data set into tiles

Copyright (c) 2020, 3D geoinformation group, Delft University of Technology

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

import pgutils
import psycopg.errors
from psycopg import sql
from psycopg.errors import Error as pgError
from click import secho

log = logging.getLogger(__name__)


def create_extent_table(conn: pgutils.PostgresConnection, srid: int, extent: sql.Identifier) -> bool:
    """Creates a table in Postgres for storing the tile index extent.
    :returns: True on success
    """
    srid = str(srid)
    if srid is None or len(srid) == 0:
        raise ValueError(f"SRID={srid} is not valid. Set it in the configuration "
                         f" file at tile_index.srid")
    query_params = {
        'extent_name': extent,
        'srid': srid
    }
    query_empty = sql.SQL("""
        CREATE TABLE {extent_name} (
            gid serial PRIMARY KEY,  
            geom geometry(POLYGON, {srid})
        );
    """)
    query = pgutils.inject_parameters(query_empty, query_params)
    try:
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except pgError as e:
        log.error(e)
        return False
    return True

def create_tx_table(conn: pgutils.PostgresConnection, tile_index, srid, drop=False) -> bool:
    """Creates an extent table in Postgres for storing the tile index extent.
    :returns: True on success
    """
    srid = str(srid)
    if srid is None or len(srid) == 0:
        raise ValueError(f"SRID={srid} is not valid. Set it in the configuration "
                         f" file at tile_index.srid")
    query_schema = sql.SQL(
        "CREATE SCHEMA IF NOT EXISTS {};"
    ).format(tile_index.schema.id)
    query_params = {
        'table': tile_index.schema + tile_index.table,
        'srid': srid,
        'gid': tile_index.field.pk.id,
        'geom': tile_index.field.geometry.id,
        'geom_sw': tile_index.field.geometry_sw_boundary.id
    }
    query_empty = sql.SQL("""
        CREATE TABLE {table}(
            {gid} text PRIMARY KEY,  
            {geom} geometry(POLYGON, {srid}),
            {geom_sw} geometry(LINESTRING, {srid})
        );
    """)
    query = pgutils.inject_parameters(query_empty, query_params)
    if drop:
        drop_query = sql.SQL(
            "DROP TABLE IF EXISTS {} CASCADE;"
        ).format(tile_index.schema + tile_index.table)
    try:
        log.debug(conn.print_query(query_schema))
        conn.send_query(query_schema)
        if drop:
            log.debug(conn.print_query(drop_query))
            conn.send_query(drop_query)
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except psycopg.errors.DuplicateTable as e:
        log.error(e)
        secho("It is not possible append the tile index to an existing "
              "table. Use --drop if you want to DROP the existing table.",
              fg='red')
        return False
    except pgError as e:
        log.error(e)
        return False
    return True

def insert_ewkt(conn, extent_table: sql.Identifier, ewkt: str) -> bool:
    """Insert an EKWT representation of a polygon into PostGIS.
    :returns: True on success
    """
    query_empty = sql.SQL("""
        INSERT INTO {extent} (geom) VALUES (ST_GeomFromEWKT({ewkt}));"""
    )
    query = pgutils.inject_parameters(query_empty, params={
        "extent": extent_table, "ewkt" : ewkt
    })
    try:
        conn.send_query(query)
    except pgError as e:
        log.error(e)
        return False
    return True


def clip_grid(conn: pgutils.PostgresConnection, tile_index: pgutils.Schema, extent: sql.Identifier) -> bool:
    """Intersect the tile_index with the extent in PostGIS and drop the
    cells from tile_index that do not intersect."""
    query_params = {
        'table_idx': tile_index.schema + tile_index.table,
        'id': tile_index.field.pk.id,
        'geometry': tile_index.field.geometry.id,
        'table_extent': extent
    }
    query_empty = sql.SQL("""
    DELETE
    FROM
        {table_idx} ti2
    WHERE
        ti2.{id} IN (
        SELECT
            ti2.{id}
        FROM
            {table_idx} ti2,
            {table_extent} n
        WHERE
            NOT st_intersects(ti2.{geometry}, n.geom));
    """)
    query = pgutils.inject_parameters(query_empty, query_params)
    try:
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except pgError as e:
        log.error(e)
        return False
    return True


def gist_on_grid(conn: pgutils.PostgresConnection, tile_index: pgutils.Schema) -> bool:
    """Create a GiST index on the tile index polygons."""
    query_params = {
        'table': tile_index.schema + tile_index.table,
        'geometry': tile_index.field.geometry.id,
        'name': pgutils.PostgresIdentifier(f"{tile_index.table}_geom_idx")
    }
    query_empty = sql.SQL("""
    CREATE INDEX IF NOT EXISTS {name} ON {table} USING gist ({geometry});
    """)
    query = pgutils.inject_parameters(query_empty, query_params)
    try:
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except pgError as e:
        log.error(e)
        return False
    query_params = {
        'table': tile_index.schema + tile_index.table,
        'geometry_sw_boundary': tile_index.field.geometry_sw_boundary,
        'name': pgutils.PostgresIdentifier(f"{tile_index.table}_geom_sw_boundary_idx")
    }
    query_empty = sql.SQL("""
    CREATE INDEX IF NOT EXISTS {name} ON
    {table}
    USING gist ({geometry_sw_boundary});
    """)
    query = pgutils.inject_parameters(query_empty, query_params)
    try:
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except pgError as e:
        log.error(e)
        return False
    return True
