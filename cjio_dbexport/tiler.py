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
from psycopg2 import sql
from psycopg2 import Error as pgError

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

