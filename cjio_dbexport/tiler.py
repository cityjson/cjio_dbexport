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
import json
from typing import TextIO, Iterable, Mapping
from psycopg2 import sql
from psycopg2 import Error as pgError

from cjio_dbexport import db

log = logging.getLogger(__name__)

def read_geojson_polygon(fo: TextIO) -> Iterable:
    """Reads a single polygon from a GeoJSON file.
    :returns: A Simple Feature representation of the polygon
    """
    polygon = list()
    # Only Polygon is allowed (no Multi-)
    gjson = json.load(fo)
    if gjson['features'][0]['geometry']['type'] != 'Polygon':
        raise ValueError(f"The first Feature in GeoJSON is "
                         f"{gjson['features'][0]['geometry']['type']}. Only "
                         f"Polygon is allowed.")
    else:
        polygon = gjson['features'][0]['geometry']['coordinates']
    return polygon

def to_ewkt(polygon, srid) -> str:
    """Creates a WKT representation of a Simple Feature polygon.
    :returns: The WKT string of ``polygon``
    """
    ring = [" ".join(map(str, i)) for i in polygon[0]]
    ewkt = f'SRID={srid};POLYGON(({",".join(ring)}))'
    return ewkt

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
    """See https://gis.stackexchange.com/a/246646

    Do https://github.com/qgis/QGIS/blob/a3ef3899c1d68b571f1e832d64b9423e5115c60b/src/analysis/processing/qgsalgorithmgrid.cpp
    f.setAttributes( QgsAttributes() << id << x1 << y1 << x2 << y2 );
    are id << left << top << right << bottom
    """

