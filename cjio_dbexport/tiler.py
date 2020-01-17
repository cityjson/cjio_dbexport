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
from typing import Mapping
from psycopg2 import sql, errors
from psycopg2 import Error as pgError
from click import secho

from cjio_dbexport import db

log = logging.getLogger(__name__)


def create_temp_table(conn: db.Db, cfg: Mapping) -> bool:
    """Creates a temp table in Postgres for storing the tile index extent.
    :returns: True on success
    """
    query_params = {
        'temp_name': sql.Identifier('extent'),
        'srid': sql.Literal(cfg['tile_index']['srid'])
    }
    query = sql.SQL("""
        CREATE TEMPORARY TABLE {temp_name}(
            gid serial PRIMARY KEY,  
            geom geometry(POLYGON, {srid})
        );
    """).format(**query_params)
    log.debug(conn.print_query(query))
    try:
        conn.send_query(query)
    except pgError as e:
        log.error(f"{e.pgcode}\t{e.pgerror}")
        return False
    return True

def create_tx_table(conn: db.Db, tile_index, srid, drop=False) -> bool:
    """Creates a temp table in Postgres for storing the tile index extent.
    :returns: True on success
    """
    srid = str(srid)
    if srid is None or len(srid) == 0:
        raise ValueError(f"SRID={srid} is not valid. Set it in the configuration "
                         f" file at tile_index.srid")
    query_schema = sql.SQL(
        "CREATE SCHEMA IF NOT EXISTS {};"
    ).format(tile_index.schema.sqlid)
    query_params = {
        'table': tile_index.schema + tile_index.table,
        'srid': sql.Literal(srid),
        'gid': tile_index.field.pk.sqlid,
        'geom': tile_index.field.geometry.sqlid
    }
    query = sql.SQL("""
        CREATE TABLE {table}(
            {gid} text PRIMARY KEY,  
            {geom} geometry(POLYGON, {srid})
        );
    """).format(**query_params)
    if drop:
        drop_query = sql.SQL(
            "DROP TABLE {} CASCADE;"
        ).format(tile_index.schema + tile_index.table)
    try:
        log.debug(conn.print_query(query_schema))
        conn.send_query(query_schema)
        if drop:
            log.debug(conn.print_query(drop_query))
            conn.send_query(drop_query)
        log.debug(conn.print_query(query))
        conn.send_query(query)
    except pgError as e:
        if e.pgcode == '42P07':
            log.error(f"{e.pgcode}\t{e.pgerror}")
            secho("It is not possible append the tile index to an existing "
                  "table. Use --drop if you want to DROP the existing table.",
                  fg='red')
        else:
            log.error(f"{e.pgcode}\t{e.pgerror}")
        return False
    return True

def insert_ewkt(conn, temp_table: sql.Identifier, ewkt: str) -> bool:
    """Insert an EKWT representation of a polygon into PostGIS.
    :returns: True on success
    """
    query = sql.SQL("""
        INSERT INTO {extent} (geom) VALUES (ST_GeomFromEWKT({ewkt}));"""
    ).format(extent=temp_table, ewkt=sql.Literal(ewkt))
    log.debug(conn.print_query(query))
    try:
        conn.send_query(query)
    except pgError as e:
        log.error(f"{e.pgcode}\t{e.pgerror}")
        return False
    return True

def create_tiles():
    """
    """

