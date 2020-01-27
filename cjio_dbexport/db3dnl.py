# -*- coding: utf-8 -*-
"""Export from the 3DNL database.

Copyright (c) 2019, 3D geoinformation group, Delft University of Technology

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
from datetime import datetime
from typing import Mapping, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from click import ClickException
from cjio import cityjson
from cjio.models import CityObject, Geometry
from psycopg2 import sql, pool, Error as pgError

from cjio_dbexport import db, utils

log = logging.getLogger(__name__)


def build_query(conn: db.Db, features: db.Schema, tile_index: db.Schema,
                tile_list=None, bbox=None, extent=None):
    """Build an SQL query for extracting CityObjects from a single table.

    ..todo: make EPSG a parameter
    """
    # Set EPSG
    epsg = 7415
    # Exclude columns from the selection
    table_fields = conn.get_fields(features.schema + features.table)
    if features.field.exclude:
        exclude = [f.string for f in features.field.exclude if f is not None]
    else:
        exclude = []
    attr_select = sql.SQL(', ').join(sql.Identifier(col) for col in table_fields
                                     if col != features.field.pk.string and
                                     col != features.field.geometry.string and
                                     col != features.field.cityobject_id.string and
                                     col not in exclude)
    # polygons subquery
    if bbox:
        log.info(f"Exporting with BBOX {bbox}")
        polygons_sub, attr_where, extent_sub = query_bbox(features, bbox, epsg)
    elif tile_list:
        log.info(f"Exporting with a list of tiles {tile_list}")
        polygons_sub, attr_where, extent_sub = query_tiles_in_list(
            features=features,
            tile_index=tile_index,
            tile_list=tile_list)
    elif extent:
        log.info(f"Exporting with polygon extent")
        ewkt = utils.to_ewkt(polygon=extent, srid=epsg)
        polygons_sub, attr_where, extent_sub = query_extent(features=features,
                                                            ewkt=ewkt)
    else:
        log.info(f"Exporting the whole database")
        polygons_sub, attr_where, extent_sub = query_all(features)

    # Main query
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'tbl': features.schema + features.table,
        'attr': attr_select,
        'where_instersects': attr_where,
        'polygons': polygons_sub,
        'extent': extent_sub,
    }


    query = sql.SQL("""
    WITH
         {extent}
         attr_in_extent AS (
         SELECT {pk} pk,
                {coid} coid,
                {attr}
         FROM {tbl} a
         {where_instersects}),
         {polygons},
         expand_points AS (
             SELECT pk,
                    (geom).PATH[1]         exterior,
                    (geom).PATH[2]         interior,
                    (geom).PATH[3]         vertex,
                    ARRAY [ST_X((geom).geom),
                        ST_Y((geom).geom),
                        ST_Z((geom).geom)] point
             FROM polygons
             WHERE (geom).PATH[3] > 1
             ORDER BY pk,
                      exterior,
                      interior,
                      vertex),
         rings AS (
             SELECT pk,
                    exterior,
                    interior,
                    ARRAY_AGG(point) geom
             FROM expand_points
             GROUP BY interior,
                      exterior,
                      pk
             ORDER BY exterior,
                      interior),
         surfaces AS (
             SELECT pk,
                    ARRAY_AGG(geom) geom
             FROM rings
             GROUP BY exterior,
                      pk
             ORDER BY exterior),
         multisurfaces AS (
             SELECT pk,
                    ARRAY_AGG(geom) geom
             FROM surfaces
             GROUP BY pk)
    SELECT b.pk,
           b.geom,
           a.*
    FROM multisurfaces b
             INNER JOIN attr_in_extent a ON
        b.pk = a.pk;
    """).format(**query_params)
    log.debug(conn.print_query(query))
    return query


def query_all(features):
    """Build a subquery of all the geometry in the table."""
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'tbl': features.schema + features.table
    }
    sql_polygons =  sql.SQL("""
    polygons AS (
        SELECT
            {pk}                      pk,
            ST_DumpPoints({geometry}) geom
        FROM 
            {tbl}
    )
    """).format(**query_params)
    sql_where_attr_intersects = sql.SQL("")
    sql_extent = sql.SQL("")
    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_bbox(features, bbox, epsg):
    """Build a subquery of the geometry in a BBOX."""
    envelope = ','.join(map(str, bbox))
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'epsg': sql.Literal(epsg),
        'xmin': sql.Literal(bbox[0]),
        'ymin': sql.Literal(bbox[1]),
        'xmax': sql.Literal(bbox[2]),
        'ymax': sql.Literal(bbox[3]),
        'tbl': features.schema + features.table
    }
    sql_polygons = sql.SQL("""
    polygons AS (
        SELECT {pk}                      pk,
               ST_DumpPoints({geometry}) geom
        FROM
            {tbl}
        WHERE ST_Intersects(
            {geometry},
            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {epsg})
            )
    )
    """).format(**query_params)
    sql_where_attr_intersects = sql.SQL("""
    WHERE ST_Intersects(
        a.{geometry},
        ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {epsg})
        )
    """).format(**query_params)
    sql_extent = sql.SQL("")
    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_extent(features, ewkt):
    """Build a subquery of the geometry in a polygon."""
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'tbl': features.schema + features.table,
        'poly': sql.Literal(ewkt),
    }
    sql_polygons = sql.SQL("""
    polygons AS (
        SELECT
            {pk}                      pk,
            ST_DumpPoints({geometry}) geom
        FROM
            {tbl}
        WHERE ST_Intersects(
            {geometry},
            {poly}
            )
    )
    """).format(**query_params)
    sql_where_attr_intersects = sql.SQL("""
    WHERE ST_Intersects(
        a.{geometry},
        {poly}
        )
    """).format(**query_params)
    sql_extent = sql.SQL("")
    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_tiles_in_list(features, tile_index, tile_list):
    """Build a subquery of the geometry in the tile list."""
    tl_tup = tuple(tile_list)
    query_params = {
        'tbl': features.schema + features.table,
        'tbl_pk': features.field.pk.sqlid,
        'tbl_coid': features.field.cityobject_id.sqlid,
        'tbl_geom': features.field.geometry.sqlid,
        'tile_index': tile_index.schema + tile_index.table,
        'tx_geom': tile_index.field.geometry.sqlid,
        'tx_pk': tile_index.field.pk.sqlid,
        'tile_list': sql.Literal(tl_tup)
    }
    sql_extent = sql.SQL("""
    extent AS (
        SELECT ST_Union({tx_geom}) geom
        FROM {tile_index}
        WHERE {tx_pk} IN {tile_list}),
    """).format(**query_params)
    sql_polygon = sql.SQL("""
    geom_in_extent AS (
        SELECT a.*
        FROM {tbl} a,
            extent t
        WHERE ST_Intersects(t.geom,
                            a.{tbl_geom})),
    polygons AS (
        SELECT {tbl_pk}                  pk,
            ST_DumpPoints({tbl_geom}) geom
        FROM geom_in_extent b)
    """).format(**query_params)
    sql_where_attr_intersects = sql.SQL("""
    ,extent t WHERE ST_Intersects(t.geom, a.{tbl_geom})
    """).format(**query_params)
    return sql_polygon, sql_where_attr_intersects, sql_extent


def with_list(conn: db.Db, tile_index: db.Schema,
              tile_list: Tuple[str]) -> Iterable[str]:
    """Select tiles based on a list of tile IDs."""
    if 'all' == tile_list[0].lower():
        log.info("Getting all tiles from the index.")
        in_index = all_in_index(conn=conn, tile_index=tile_index)
    else:
        log.info("Verifying if the provided tiles are in the index.")
        in_index = tiles_in_index(conn=conn, tile_index=tile_index,
                                  tile_list=tile_list)
    if len(in_index) == 0:
        raise AttributeError("None of the provided tiles are present in the"
                             " index.")
    else:
        return in_index


def tiles_in_index(conn: db.Db, tile_index: db.Schema,
                   tile_list: Tuple[str]) -> Iterable[str]:
    """Return the tile IDs that are present in the tile index."""
    if not isinstance(tile_list, tuple):
        tile_list = tuple(tile_list)
        log.debug(f"tile_list was not a tuple, casted to tuple {tile_list}")

    query_params = {
        'tiles': sql.Literal(tile_list),
        'index_': tile_index.schema + tile_index.table,
        'tile': tile_index.field.pk.sqlid
    }
    query = sql.SQL("""
    SELECT DISTINCT {tile}
    FROM {index_}
    WHERE {tile} IN {tiles}
    """).format(**query_params)
    log.debug(conn.print_query(query))
    in_index = [t[0] for t in conn.get_query(query)]
    diff = set(tile_list) - set(in_index)
    if len(diff) > 0:
        log.warning(f"The provided tile IDs {diff} are not in the index, "
                    f"they are skipped.")
    return in_index

def all_in_index(conn: db.Db, tile_index: db.Schema) -> Iterable[str]:
    """Get all tile IDs from the tile index."""
    query_params = {
        'index_': tile_index.schema + tile_index.table,
        'tile': tile_index.field.pk.sqlid
    }
    query = sql.SQL("""
    SELECT DISTINCT {tile} FROM {index_}
    """).format(**query_params)
    log.debug(conn.print_query(query))
    return [t[0] for t in conn.get_query(query)]

def parse_polygonz(wkt_polygonz):
    """Parses a POLYGON Z array of WKT into CityJSON Surface"""
    # match: 'POLYGON Z (<match everything in here>)'
    outer_pat = re.compile(r"(?<=POLYGON Z \().*(?!$)")
    # match: '(<match everything in here>)'
    ring_pat = re.compile(r"\(([^)]+)\)")
    outer = outer_pat.findall(wkt_polygonz)
    if len(outer) > 0:
        rings = ring_pat.findall(outer[0])
        for ring in rings:
            pts = [tuple(map(float, pt.split()))
                   for pt in ring.split(',')]
            yield pts[1:]  # WKT repeats the first vertex
    else:
        log.error("Not a POLYGON Z")


def export(conn_cfg: Mapping, tile_index: db.Schema, cityobject_type: Mapping,
           threads=None,
           tile_list=None, bbox=None, extent=None):
    """Export a table from PostgreSQL. Multithreading, with connection pooling."""
    # see: https://realpython.com/intro-to-python-threading/
    # see: https://stackoverflow.com/a/39310039
    # Need one thread per table
    if threads is None:
        threads = sum(len(cotables) for cotables in cityobject_type.values())
    log.debug(f"Number of threads={threads}")
    conn_pool = pool.ThreadedConnectionPool(minconn=1,
                                            maxconn=threads,
                                            **conn_cfg)
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_table = {}
            for cotype, cotables in cityobject_type.items():
                # Need a thread for each of these
                for cotable in cotables:
                    tablename = cotable['table']
                    # Need a connection from the pool per thread
                    conn = db.Db(conn=conn_pool.getconn(key=(cotype, tablename)))
                    # Need a connection and thread for each of these
                    log.info(f"CityObject {cotype} from table {cotable['table']}")
                    features = db.Schema(cotable)
                    query = build_query(conn=conn, features=features,
                                        tile_index=tile_index, tile_list=tile_list,
                                        bbox=bbox, extent=extent)
                    # Schedule the DB query for execution and store the returned
                    # Future together with the cotype and table name
                    future = executor.submit(conn.get_dict, query)
                    future_to_table[future] = (cotype, tablename)
                    # If I put away the connection here, then it locks the main
                    # thread and it becomes like using a single connection.
                    # conn_pool.putconn(conn=conn.conn, key=(cotype, tablename),
                    #                   close=True)
            for future in as_completed(future_to_table):
                cotype, tablename = future_to_table[future]
                try:
                    # Note that resultset can be []
                    yield (cotype, cotable['table']), future.result()
                except pgError as e:
                    log.error(f"{e.pgcode}\t{e.pgerror}")
                    raise ClickException(f"Could not query {cotable}. Check the "
                                         f"logs for details.")
    finally:
        conn_pool.closeall()

### start Multithreading optimisation tests ---

def _export_no_pool(conn_cfg: Mapping, tile_index: db.Schema, cityobject_type: Mapping,
           threads=None,
           tile_list=None, bbox=None, extent=None):
    """Export a table from PostgreSQL. Multithreading, without connection pooling."""
    # see: https://realpython.com/intro-to-python-threading/
    # see: https://stackoverflow.com/a/39310039
    # Need one thread per table
    if threads is None:
        threads = sum(len(cotables) for cotables in cityobject_type.values())
    log.debug(f"Number of threads={threads}")
    conn = db.Db(**conn_cfg)
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_table = {}
            for cotype, cotables in cityobject_type.items():
                # Need a thread for each of these
                for cotable in cotables:
                    tablename = cotable['table']
                    # Need a connection from the pool per thread
                    # conn = db.Db(conn=conn_pool.getconn(key=(cotype, tablename)))
                    # Need a connection and thread for each of these
                    log.info(f"CityObject {cotype} from table {cotable['table']}")
                    features = db.Schema(cotable)
                    query = build_query(conn=conn, features=features,
                                        tile_index=tile_index, tile_list=tile_list,
                                        bbox=bbox, extent=extent)
                    # Schedule the DB query for execution and store the returned
                    # Future together with the cotype and table name
                    future = executor.submit(conn.get_dict, query)
                    future_to_table[future] = (cotype, tablename)
                    # conn_pool.putconn(conn=conn.conn, key=(cotype, tablename),
                    #                   close=False)
            for future in as_completed(future_to_table):
                cotype, tablename = future_to_table[future]
                try:
                    # Note that resultset can be []
                    yield (cotype, cotable['table']), future.result()
                except pgError as e:
                    log.error(f"{e.pgcode}\t{e.pgerror}")
                    raise ClickException(f"Could not query {cotable}. Check the "
                                         f"logs for details.")
    finally:
        conn.close()

def _export_single(conn: db.Db, cfg: Mapping, tile_list=None, bbox=None,
           extent=None):
    """Export a table from PostgreSQL. Single thread, single connection."""
    # Need a thread per tile
    tile_index = db.Schema(cfg['tile_index'])
    for cotype, cotables in cfg['cityobject_type'].items():
        # Need a thread for each of these
        for cotable in cotables:
            # Need a connection and thread for each of these
            log.info(f"CityObject {cotype} from table {cotable['table']}")
            features = db.Schema(cotable)
            query = build_query(conn=conn, features=features,
                                tile_index=tile_index, tile_list=tile_list,
                                bbox=bbox, extent=extent)
            try:
                tabledata =  conn.get_dict(query)
            except pgError as e:
                log.error(f"{e.pgcode}\t{e.pgerror}")
                raise ClickException(f"Could not query {cotable}. Check the "
                                     f"logs for details.")
            # Note that resultset can be []
            yield (cotype,cotable['table']), tabledata

### end Multithreading optimisation test ---

def table_to_cityobjects(tabledata, cotype: str, geomtype: str):
    """Converts a database record to a CityObject."""
    for record in tabledata:
        coid = record['coid']
        co = CityObject(id=coid)
        # Parse the geometry
        # TODO: refactor geometry parsing into a function
        geom = Geometry(type=geomtype, lod=1)
        if geomtype == 'Solid':
            solid = [record['geom'],]
            geom.boundaries = solid
        elif geomtype == 'MultiSurface':
            geom.boundaries = record['geom']
        co.geometry.append(geom)
        # Parse attributes
        for key, attr in record.items():
            if key != 'pk' and key != 'geom' and key != 'coid':
                if isinstance(attr, datetime):
                    co.attributes[key] = attr.isoformat()
                else:
                    co.attributes[key] = attr
        # Set the CityObject type
        co.type = cotype
        yield coid, co


def dbexport_to_cityobjects(dbexport):
    for coinfo, tabledata in dbexport:
        cotype, cotable = coinfo
        log.info(f"Generating CityObject {cotype} from table {cotable}")
        if cotype.lower() == 'building':
            geomtype = 'Solid'
        else:
            # FIXME: because CompositeSurface is not supported at the moment for semantic surfaces in cjio.models
            geomtype = 'MultiSurface'

        # Loop through the whole tabledata and create the CityObjects
        cityobject_generator = table_to_cityobjects(
            tabledata=tabledata, cotype=cotype, geomtype=geomtype)
        for coid, co in cityobject_generator:
            yield coid, co


def convert(dbexport):
    """Convert the exported citymodel to CityJSON."""
    # Set EPSG
    epsg = 7415
    cm = cityjson.CityJSON()
    cm.cityobjects = dict(dbexport_to_cityobjects(dbexport))
    cityobjects, vertex_lookup = cm.reference_geometry()
    cm.add_to_j(cityobjects, vertex_lookup)
    cm.update_bbox()
    cm.set_epsg(epsg)
    log.info(f"Exported CityModel:\n{cm}")
    return cm


def execute(cotypes):
    dbexport = export()
    cm = convert(dbexport)