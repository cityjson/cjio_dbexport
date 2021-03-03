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
from concurrent.futures._base import as_completed
from concurrent.futures.process import ProcessPoolExecutor
from datetime import date, time, datetime, timedelta
from typing import Mapping, Sequence, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path

import click
from click import ClickException, echo
from cjio import cityjson
from cjio.models import CityObject, Geometry
from psycopg2 import sql, pool, Error as pgError

from cjio_dbexport import settings, db, utils

log = logging.getLogger(__name__)


def get_tile_list(cfg: Mapping, tiles: List) -> List:
    conn = db.Db(**cfg['database'])
    if not conn.create_functions():
        raise BaseException(
            "Could not create the required functions in PostgreSQL, check the logs for details")
    tile_index = db.Schema(cfg['tile_index'])
    try:
        tile_list = with_list(conn=conn, tile_index=tile_index,
                                     tile_list=tiles)
        log.info(f"Found {len(tile_list)} tiles in the tile index.")

    except BaseException as e:
        raise BaseException(f"Could not generate tile_list. Check the logs for details.\n{e}")
    finally:
        conn.close()
    return tile_list


def export_tiles_multiprocess(cfg: Mapping, jobs: int, path: Path, tile_list: List) -> Mapping:
    failed = []
    futures = []
    if not path.exists():
        raise NotADirectoryError(str(path))
    with ProcessPoolExecutor(max_workers=jobs) as executor:
        for tile in tile_list:
            filepath = (path / str(tile)).with_suffix('.json')
            futures.append(executor.submit(export, tile, filepath,
                                           cfg))

        for i, future in enumerate(as_completed(futures)):
            success, filepath = future.result()
            if success:
                log.info(
                    f"[{i + 1}/{len(tile_list)}] Saved {filepath.name}")
            else:
                failed.append(filepath.stem)
        log.info(
            f"Done. Exported {len(tile_list) - len(failed)} tiles. "
            f"Failed {len(failed)} tiles: {failed}")
        return {"exported": len(tile_list) - len(failed),
                "nr_failed:": len(failed),
                "failed": failed}


def export(tile, filepath, cfg):
    """Export a tile from PostgreSQL, convert to CityJSON and write to file."""
    try:
        dbexport = query(
            conn_cfg=cfg["database"],
            tile_index=cfg["tile_index"],
            cityobject_type=cfg["cityobject_type"],
            threads=1,
            tile_list=(tile,),
        )
    except BaseException as e:
        log.error(f"Failed to export tile {str(tile)}\n{e}")
        return False, filepath
    try:
        cm = to_citymodel(dbexport, cfg=cfg, compress=True, important_digits=3)
    finally:
        del dbexport
    if cm is not None:
        cm.j["metadata"]["fileIdentifier"] = filepath.name
        try:
            with open(filepath, "w") as fout:
                json_str = json.dumps(cm.j, separators=(',', ':'))
                fout.write(json_str)
            return True, filepath
        except IOError as e:
            log.error(f"Invalid output file: {filepath}\n{e}")
            return False, filepath
        finally:
            del cm
    else:
        log.error(
            f"Failed to create CityJSON from {filepath.stem},"
            f" check the logs for details."
        )
        return False, filepath


def to_citymodel(dbexport, cfg, compress: bool = True, important_digits: int = 3):
    try:
        cm = convert(dbexport, cfg=cfg)
    except BaseException as e:
        log.error(f"Failed to convert database export to CityJSON\n{e}")
        return None
    if cm and compress:
        try:
            cm.compress(important_digits=important_digits)
        except BaseException as e:
            log.error(f"Failed to compress cityjson\n{e}")
            return None
        return cm
    elif cm and not compress:
        try:
            cm.remove_duplicate_vertices()
        except BaseException as e:
            log.error(f"Failed to remove duplicate vertices\n{e}")
            return None
        try:
            cm.remove_orphan_vertices()
        except BaseException as e:
            log.error(f"Failed to remove orphan vertices\n{e}")
            return None
        return cm


def convert(dbexport, cfg):
    """Convert the exported citymodel to CityJSON. """
    # Set EPSG
    epsg = 7415
    # Set rounding for floating point attributes
    cfg["rounding"] = 4
    log.info(
        f"Floating point attributes are rounded up to {cfg['rounding']} decimal digits")
    cm = cityjson.CityJSON()
    cm.cityobjects = dict(dbexport_to_cityobjects(dbexport, cfg))
    log.debug("Referencing geometry")
    cityobjects, vertex_lookup = cm.reference_geometry()
    log.debug("Adding to json")
    cm.add_to_j(cityobjects, vertex_lookup)
    log.debug("Updating metadata")
    cm.update_metadata(overwrite=True, new_uuid=True)
    log.debug("Setting EPSG")
    cm.set_epsg(epsg)
    log.info(f"Exported CityModel:\n{cm}")
    return cm


def dbexport_to_cityobjects(dbexport, cfg):
    for coinfo, tabledata in dbexport:
        cotype, cotable = coinfo
        cfg_geom = None
        for _c in cfg["cityobject_type"][cotype]:
            if _c["table"] == cotable:
                cfg_geom = _c["field"]["geometry"]
                cfg_geom['lod'] = _c["field"].get('lod')
                cfg_geom['semantics'] = _c["field"].get('semantics')
                cfg_geom['semantics_mapping'] = cfg.get('semantics_mapping')
        # Loop through the whole tabledata and create the CityObjects
        cityobject_generator = table_to_cityobjects(
            tabledata=tabledata, cotype=cotype, cfg_geom=cfg_geom,
            rounding=cfg['rounding']
        )
        for coid, co in cityobject_generator:
            yield coid, co


def table_to_cityobjects(tabledata, cotype: str, cfg_geom: dict, rounding: int):
    """Converts a database record to a CityObject."""
    for record in tabledata:
        coid = record["coid"]
        co = CityObject(id=coid)
        # Parse the geometry
        co.geometry = record_to_geometry(record, cfg_geom)
        # Parse attributes, except special fields that serve some purpose,
        # eg. primary key (pk) or cityobject ID (coid)
        special_fields = ['pk', 'coid', cfg_geom['lod'], cfg_geom['semantics']]
        for key, attr in record.items():
            if key not in special_fields and "geom_" not in key:
                if isinstance(attr, float):
                    co.attributes[key] = round(attr, rounding)
                elif isinstance(attr, date) or isinstance(attr, time) or isinstance(attr, datetime):
                    co.attributes[key] = attr.isoformat()
                elif isinstance(attr, timedelta):
                    co.attributes[key] = str(attr)
                else:
                    co.attributes[key] = attr
        # Set the CityObject type
        co.type = cotype
        yield coid, co


def record_to_geometry(record: Mapping, cfg_geom: dict) -> Sequence[Geometry]:
    """Create a CityJSON Geometry from a boundary array that was retrieved from
    Postgres.
    """
    geometries = []
    lod_column = cfg_geom.get('lod')
    semantics_column = cfg_geom.get('semantics')
    for lod_key in [k for k in cfg_geom if k != 'lod' and k != 'semantics' and k != 'semantics_mapping']:
        if lod_column:
            lod = record[lod_column]
        else:
            lod = utils.parse_lod_value(lod_key)
        lod_float = round(float(lod), 1)
        geomtype = cfg_geom[lod_key]["type"]
        geom = Geometry(type=geomtype, lod=lod_float)
        if geomtype == "Solid":
            solid = [
                record.get(settings.geom_prefix + lod_key),
            ]
            geom.boundaries = solid
        elif geomtype == "MultiSurface":
            geom.boundaries = record.get(settings.geom_prefix + lod_key)
        if semantics_column:
            geom.surfaces = record_to_surfaces(
                geomtype=geomtype,
                boundary=geom.boundaries,
                semantics=record[semantics_column],
                semantics_mapping=cfg_geom['semantics_mapping']
            )
        geometries.append(geom)
    return geometries


def record_to_surfaces(geomtype: str, boundary: Sequence,
                        semantics: Sequence[int], semantics_mapping: dict) -> dict:
    """Create a CityJSON Semantic Surface object from an array of labels and a
    CityJSON geometry representation.
    """
    surfaces = {}
    for key, type in semantics_mapping.items():
        surfaces[key] = {'surface_idx': [], 'type': type}
    if geomtype == "Solid":
        if len(boundary) > 1:
            log.warning("Cannot assign semantics to Solids with inner shell(s)")
        shell = boundary[0]
        if len(shell) != len(semantics):
            log.warning("Encountered unequal sized geometry shell and semantics arrays")
        else:
            for i, srf in enumerate(shell):
                surfaces[semantics[i]]['surface_idx'].append([0,i])
    elif geomtype == "MultiSurface":
        for i, srf in enumerate(boundary):
            surfaces[semantics[i]]['surface_idx'].append(i)
    return {sem: idx for sem, idx in surfaces.items() if len(idx['surface_idx']) > 0}


def query(
    conn_cfg: Mapping,
    tile_index: Mapping,
    cityobject_type: Mapping,
    threads=None,
    tile_list=None,
    bbox=None,
    extent=None,
):
    """Export a table from PostgreSQL. Multithreading, with connection pooling."""
    # see: https://realpython.com/intro-to-python-threading/
    # see: https://stackoverflow.com/a/39310039
    # Need one thread per table
    if threads is None:
        threads = sum(len(cotables) for cotables in cityobject_type.values())
    if threads == 1:
        log.debug(f"Running on a single thread.")
        conn = db.Db(**conn_cfg)
        try:
            for cotype, cotables in cityobject_type.items():
                for cotable in cotables:
                    tablename = cotable["table"]
                    log.debug(f"CityObject {cotype} from table {tablename}")
                    features = db.Schema(cotable)
                    tx = db.Schema(tile_index)
                    sql_query = build_query(
                        conn=conn,
                        features=features,
                        tile_index=tx,
                        tile_list=tile_list,
                        bbox=bbox,
                        extent=extent,
                    )
                    try:
                        # Note that resultset can be []
                        yield (cotype, tablename), conn.get_dict(sql_query)
                    except pgError as e:
                        log.error(f"{e.pgcode}\t{e.pgerror}")
                        raise ClickException(
                            f"Could not query {cotable}. Check the "
                            f"logs for details."
                        )
        finally:
            conn.close()
    elif threads > 1:
        log.debug(f"Running with ThreadPoolExecutor, nr. of threads={threads}")
        pool_size = sum(len(cotables) for cotables in cityobject_type.values())
        conn_pool = pool.ThreadedConnectionPool(
            minconn=1, maxconn=pool_size + 1, **conn_cfg
        )
        try:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                future_to_table = {}
                for cotype, cotables in cityobject_type.items():
                    # Need a thread for each of these
                    for cotable in cotables:
                        tablename = cotable["table"]
                        # Need a connection from the pool per thread
                        conn = db.Db(conn=conn_pool.getconn(key=(cotype, tablename)))
                        # Need a connection and thread for each of these
                        log.debug(f"CityObject {cotype} from table {cotable['table']}")
                        features = db.Schema(cotable)
                        tx = db.Schema(tile_index)
                        sql_query = build_query(
                            conn=conn,
                            features=features,
                            tile_index=tx,
                            tile_list=tile_list,
                            bbox=bbox,
                            extent=extent,
                        )
                        # Schedule the DB query for execution and store the returned
                        # Future together with the cotype and table name
                        future = executor.submit(conn.get_dict, sql_query)
                        future_to_table[future] = (cotype, tablename)
                        # If I put away the connection here, then it locks the main
                        # thread and it becomes like using a single connection.
                        # conn_pool.putconn(conn=conn.conn, key=(cotype, tablename),
                        #                   close=True)
                for future in as_completed(future_to_table):
                    cotype, tablename = future_to_table[future]
                    try:
                        # Note that resultset can be []
                        yield (cotype, tablename), future.result()
                    except pgError as e:
                        log.error(f"{e.pgcode}\t{e.pgerror}")
                        raise ClickException(
                            f"Could not query {tablename}. Check the "
                            f"logs for details."
                        )
        finally:
            conn_pool.closeall()
    else:
        raise ValueError(f"Number of threads must be greater than 0.")


def build_query(
    conn: db.Db,
    features: db.Schema,
    tile_index: db.Schema,
    tile_list=None,
    bbox=None,
    extent=None,
):
    """Build an SQL query for extracting CityObjects from a single table.

    ..todo: make EPSG a parameter
    """
    # Set EPSG
    epsg = 7415
    # Exclude columns from the selection
    table_fields = conn.get_fields(features.schema + features.table)
    if 'exclude' in features.field._Schema__data:
        exclude = [f.string for f in features.field.exclude if f is not None]
    else:
        exclude = []
    geom_cols = [
        getattr(features.field.geometry, lod).name.string
        for lod in features.field.geometry.keys()
    ]
    attr_select = sql.SQL(", ").join(
        sql.Identifier(col)
        for col in table_fields
        if col != features.field.pk.string
        and col not in geom_cols
        and col != features.field.cityobject_id.string
        and col not in exclude
    )
    # polygons subquery
    if bbox:
        log.info(f"Exporting with BBOX {bbox}")
        polygons_sub, attr_where, extent_sub = query_bbox(features, bbox, epsg)
    elif tile_list:
        log.info(f"Exporting with a list of tiles {tile_list}")
        polygons_sub, attr_where, extent_sub = query_tiles_in_list(
            features=features, tile_index=tile_index, tile_list=tile_list
        )
    elif extent:
        log.info(f"Exporting with polygon extent")
        ewkt = utils.to_ewkt(polygon=extent, srid=epsg)
        polygons_sub, attr_where, extent_sub = query_extent(
            features=features, ewkt=ewkt
        )
    else:
        log.info(f"Exporting the whole database")
        polygons_sub, attr_where, extent_sub = query_all(features=features)

    # Main query
    query_params = {
        "pk": features.field.pk.sqlid,
        "coid": features.field.cityobject_id.sqlid,
        "tbl": features.schema + features.table,
        "attr": attr_select,
        "where_instersects": attr_where,
        "extent": extent_sub,
        "polygons": polygons_sub,
    }

    query = sql.SQL(
        """
    WITH
         {extent}
         attr_in_extent AS (
             SELECT {pk} pk,
                    {coid} coid,
                    {attr}
             FROM {tbl} a
             {where_instersects}
         ),
         {polygons}
    SELECT *
    FROM polygons b
             INNER JOIN attr_in_extent a ON
        b.pk = a.pk;
    """
    ).format(**query_params)
    log.debug(conn.print_query(query))
    return query


def query_all(features) -> Tuple[sql.Composed, ...]:
    """Build a subquery of all the geometry in the table."""
    query_params = {
        "pk": features.field.pk.sqlid,
        "coid": features.field.cityobject_id.sqlid,
        "tbl": features.schema + features.table,
        "geometries": sql_cast_geometry(features),
    }

    sql_polygons = sql.SQL(
        """
    polygons AS (
        SELECT
            {pk}    pk,
            {geometries}
        FROM 
            {tbl}
    )
    """
    ).format(**query_params)

    sql_where_attr_intersects = sql.Composed("")

    sql_extent = sql.Composed("")

    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_bbox(
    features: db.Schema, bbox: Sequence[float], epsg: int
) -> Tuple[sql.Composed, ...]:
    """Build a subquery of the geometry in a BBOX."""
    # One geometry column is enough to restrict the selection to the BBOX
    lod = list(features.field.geometry.keys())[0]
    query_params = {
        "pk": features.field.pk.sqlid,
        "coid": features.field.cityobject_id.sqlid,
        "geometries": sql_cast_geometry(features),
        "geometry_0": getattr(features.field.geometry, lod).name.sqlid,
        "epsg": sql.Literal(epsg),
        "xmin": sql.Literal(bbox[0]),
        "ymin": sql.Literal(bbox[1]),
        "xmax": sql.Literal(bbox[2]),
        "ymax": sql.Literal(bbox[3]),
        "tbl": features.schema + features.table,
    }

    sql_polygons = sql.SQL(
        """
    polygons AS (
        SELECT {pk}     pk,
               {geometries}
        FROM
            {tbl}
        WHERE ST_3DIntersects(
            {geometry_0},
            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {epsg})
            )
    )
    """
    ).format(**query_params)

    sql_where_attr_intersects = sql.SQL(
        """
    WHERE ST_3DIntersects(
        a.{geometry_0},
        ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {epsg})
        )
    """
    ).format(**query_params)

    sql_extent = sql.Composed("")

    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_extent(features: db.Schema, ewkt: str) -> Tuple[sql.Composed, ...]:
    """Build a subquery of the geometry in a polygon."""
    # One geometry column is enough to restrict the selection to the BBOX
    lod = list(features.field.geometry.keys())[0]
    query_params = {
        "pk": features.field.pk.sqlid,
        "coid": features.field.cityobject_id.sqlid,
        "geometries": sql_cast_geometry(features),
        "geometry_0": getattr(features.field.geometry, lod).name.sqlid,
        "tbl": features.schema + features.table,
        "poly": sql.Literal(ewkt),
    }

    sql_polygons = sql.SQL(
        """
    polygons AS (
        SELECT
            {pk}    pk,
            {geometries}
        FROM
            {tbl}
        WHERE ST_3DIntersects(
            {geometry_0},
            {poly}
            )
    )
    """
    ).format(**query_params)

    sql_where_attr_intersects = sql.SQL(
        """
    WHERE ST_3DIntersects(
        a.{geometry_0},
        {poly}
        )
    """
    ).format(**query_params)

    sql_extent = sql.Composed("")

    return sql_polygons, sql_where_attr_intersects, sql_extent


def query_tiles_in_list(
    features: db.Schema, tile_index: db.Schema, tile_list: Sequence[str]
) -> Tuple[sql.Composed, ...]:
    """Build a subquery of the geometry in the tile list."""
    tl_tup = tuple(tile_list)
    # One geometry column is enough to restrict the selection to the BBOX
    lod = list(features.field.geometry.keys())[0]
    query_params = {
        "tbl": features.schema + features.table,
        "tbl_pk": features.field.pk.sqlid,
        "tbl_coid": features.field.cityobject_id.sqlid,
        "tbl_geom": getattr(features.field.geometry, lod).name.sqlid,
        "geometries": sql_cast_geometry(features),
        "tile_index": tile_index.schema + tile_index.table,
        "tx_geom": tile_index.field.geometry.sqlid,
        "tx_pk": tile_index.field.pk.sqlid,
        "tile_list": sql.Literal(tl_tup),
    }

    sql_extent = sql.SQL(
        """
    extent AS (
        SELECT ST_Union({tx_geom}) geom
        FROM {tile_index}
        WHERE {tx_pk} IN {tile_list}),
    """
    ).format(**query_params)

    sql_polygon = sql.SQL(
        """
    geom_in_extent AS (
        SELECT a.*
        FROM {tbl} a,
            extent t
        WHERE ST_3DIntersects(t.geom,
                            a.{tbl_geom})),
    polygons AS (
        SELECT 
            {tbl_pk} pk,
            {geometries}
        FROM geom_in_extent b)
    """
    ).format(**query_params)

    sql_where_attr_intersects = sql.SQL(
        """
    ,extent t WHERE ST_3DIntersects(t.geom, a.{tbl_geom})
    """
    ).format(**query_params)

    return sql_polygon, sql_where_attr_intersects, sql_extent


def with_list(conn: db.Db, tile_index: db.Schema, tile_list: Tuple[str]) -> List[str]:
    """Select tiles based on a list of tile IDs."""
    if "all" == tile_list[0].lower():
        log.info("Getting all tiles from the index.")
        in_index = all_in_index(conn=conn, tile_index=tile_index)
    else:
        log.info("Verifying if the provided tiles are in the index.")
        in_index = tiles_in_index(conn=conn, tile_index=tile_index, tile_list=tile_list)
    if len(in_index) == 0:
        raise AttributeError("None of the provided tiles are present in the" " index.")
    else:
        return in_index


def tiles_in_index(
    conn: db.Db, tile_index: db.Schema, tile_list: Tuple[str]
) -> Tuple[List[str], List[str]]:
    """Return the tile IDs that are present in the tile index."""
    if not isinstance(tile_list, tuple):
        tile_list = tuple(tile_list)
        log.debug(f"tile_list was not a tuple, casted to tuple {tile_list}")

    query_params = {
        "tiles": sql.Literal(tile_list),
        "index_": tile_index.schema + tile_index.table,
        "tile": tile_index.field.pk.sqlid,
    }
    query = sql.SQL(
        """
    SELECT DISTINCT {tile}
    FROM {index_}
    WHERE {tile} IN {tiles}
    """
    ).format(**query_params)
    log.debug(conn.print_query(query))
    # FIXME: should create a tuple here or not? see also 'with_list'
    in_index = [t[0] for t in conn.get_query(query)]
    not_found = set(tile_list) - set(in_index)
    if len(not_found) > 0:
        log.warning(
            f"The provided tile IDs {not_found} are not in the index, "
            f"they are skipped."
        )
    return in_index


def all_in_index(conn: db.Db, tile_index: db.Schema) -> List[str]:
    """Get all tile IDs from the tile index."""
    query_params = {
        "index_": tile_index.schema + tile_index.table,
        "tile": tile_index.field.pk.sqlid,
    }
    query = sql.SQL(
        """
    SELECT DISTINCT {tile} FROM {index_}
    """
    ).format(**query_params)
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
            pts = [tuple(map(float, pt.split())) for pt in ring.split(",")]
            yield pts[1:]  # WKT repeats the first vertex
    else:
        log.error("Not a POLYGON Z")


def sql_cast_geometry(features: db.Schema) -> sql.Composed:
    """Create a clause for SELECT statements for the geometry columns.

    For each geometry column in the table (one column per LoD) that is mapped in
    the configuration file, prepare the clauses for the SELECT statement.

    :return: An SQL snippent for example:
        'cjdb_multipolygon_to_multisurface(wkb_geometry_lod1) geom_lod1,
         cjdb_multipolygon_to_multisurface(wkb_geometry_lod2) geom_lod2'
    """
    lod_fields = [
        sql.SQL("cjdb_multipolygon_to_multisurface({geom_field}) {geom_alias}").format(
            geom_field=getattr(features.field.geometry, lod).name.sqlid,
            geom_alias=sql.Identifier(settings.geom_prefix + lod),
        )
        for lod in features.field.geometry.keys()
    ]
    return sql.SQL(",").join(lod_fields)
