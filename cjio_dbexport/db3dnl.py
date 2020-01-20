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
from typing import Mapping, Iterable

from click import ClickException
from cjio import cityjson
from cjio.models import CityObject, Geometry
from psycopg2 import sql
from psycopg2 import Error as pgError

from cjio_dbexport import db, utils

log = logging.getLogger(__name__)


def build_query(conn: db.Db, features: db.Schema, tile_index: db.Schema,
                tile_list=None, bbox=None, extent=None):
    """Build an SQL query for extracting CityObjects from a single table.

    ..todo: make EPSG a parameter

    :param conn:
    :param cfg:
    :param features:
    :param bbox:
    :return:
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
        polygons_sub = query_bbox(features, bbox, epsg)
    elif tile_list:
        log.info(f"Exporting with a list of tiles {tile_list}")
        polygons_sub = query_tiles_in_list(features=features,
                                           tile_index=tile_index,
                                           tile_list=tile_list)
    elif extent:
        log.info(f"Exporting with polygon extent")
        ewkt = utils.to_ewkt(polygon=extent, srid=epsg)
        polygons_sub = query_extent(features=features,
                                    ewkt=ewkt)
    else:
        log.info(f"Exporting the whole database")
        polygons_sub = query_all(features)

    # Main query
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'tbl': features.schema + features.table,
        'attr': attr_select,
        'polygons': polygons_sub
    }

    query = sql.SQL("""
    WITH attrs AS (
        SELECT
            {pk} pk,
            {attr}
        FROM
            {tbl}
    ),
    {polygons},
    boundary AS (
        SELECT
            pk,
            ARRAY_AGG(ST_ASTEXT(geom)) geom,
            coid
        FROM
            polygons
        GROUP BY
            pk, coid
    )
    SELECT
        b.pk,
        b.geom,
        b.coid,
        a.*
    FROM
        boundary b
    INNER JOIN attrs a ON
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
    return sql.SQL("""
    polygons AS (
        SELECT
            {pk} pk,
            (ST_Dump({geometry})).geom,
            {coid} coid
        FROM
            {tbl}
    )
    """).format(**query_params)


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
    return sql.SQL("""
    polygons AS (
        SELECT
            {pk} pk,
            (ST_Dump({geometry})).geom,
            {coid} coid
        FROM
            {tbl}
        WHERE ST_Intersects(
            {geometry},
            ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, {epsg})
            )
    )
    """).format(**query_params)


def query_extent(features, ewkt):
    """Build a subquery of the geometry in a polygon."""
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'tbl': features.schema + features.table,
        'poly': sql.Literal(ewkt),
    }
    return sql.SQL("""
    polygons AS (
        SELECT
            {pk} pk,
            (ST_Dump({geometry})).geom,
            {coid} coid
        FROM
            {tbl}
        WHERE ST_Intersects(
            {geometry},
            {poly}
            )
    )
    """).format(**query_params)


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
    return sql.SQL("""
    extent AS (
        SELECT
            st_union({tx_geom}) geom
        FROM
            {tile_index}
        WHERE
            {tx_pk} IN {tile_list}
    ),
    sub AS (
        SELECT
            a.*
        FROM
            {tbl} a,
            extent t
        WHERE
            st_intersects(t.geom,
            a.{tbl_geom})
    ),
    polygons AS (
        SELECT
            {tbl_pk} pk,
            (ST_Dump({tbl_geom})).geom,
            {tbl_coid} coid
        FROM
            sub
    )
    """).format(**query_params)


def with_list(conn: db.Db, tile_index: db.Schema,
              tile_list: Iterable[str]) -> Iterable[str]:
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
                   tile_list: Iterable[str]) -> Iterable[str]:
    """Return the tile IDs that are present in the tile index."""
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


def export(conn: db.Db, cfg: Mapping, tile_list=None, bbox=None,
           extent=None):
    """Export a table to CityModel

    :param conn:
    :param cfg:
    :param bbox:
    :return: A citymodel of :py:class:`cityjson.CityJSON`
    """
    # Set EPSG
    epsg = 7415
    cm = cityjson.CityJSON()
    tile_index = db.Schema(cfg['tile_index'])
    for cotype, cotables in cfg['cityobject_type'].items():
        for cotable in cotables:
            log.info(f"CityObject {cotype} from table {cotable['table']}")
            if cotype.lower() == 'building':
                geomtype = 'Solid'
            else:
                # FIXME: because CompositeSurface is not supported at the moment for semantic surfaces in cjio.models
                geomtype = 'MultiSurface'

            features = db.Schema(cotable)

            query = build_query(conn=conn, features=features,
                                tile_index=tile_index, tile_list=tile_list,
                                bbox=bbox, extent=extent)
            try:
                resultset = conn.get_dict(query)
            except pgError as e:
                log.error(f"{e.pgcode}\t{e.pgerror}")
                raise ClickException(f"Could not query {cotable}. Check the "
                                     f"logs for details.")

            # Loop through the whole resultset and create the CityObjects
            for record in resultset:
                # I expect this order of fields:
                #   0 - cityobject ID
                #   1 - geometry
                #   2 < attributes
                co = CityObject(id=record['coid'])

                # Parse the geometry
                # TODO: refactor geometry parsing into a function
                geom = Geometry(type=geomtype, lod=1)
                if geomtype == 'Solid':
                    solid = []
                    outer_shell = []
                    for wkt_polyz in record['geom']:
                        surface = parse_polygonz(wkt_polyz)
                        # OPTIMISE: make use of the generator down the line
                        outer_shell.append(list(surface))
                    solid.append(outer_shell)
                    geom.boundaries = solid
                elif geomtype == 'MultiSurface':
                    outer_shell = []
                    for wkt_polyz in record['geom']:
                        surface = parse_polygonz(wkt_polyz)
                        # OPTIMISE: make use of the generator down the line
                        outer_shell.append(list(surface))
                    geom.boundaries = outer_shell
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

                # Add the CO to the CityModel
                cm.cityobjects[co.id] = co

    cityobjects, vertex_lookup = cm.reference_geometry()
    cm.add_to_j(cityobjects, vertex_lookup)
    cm.update_bbox()
    cm.set_epsg(epsg)
    log.info(f"Exported CityModel:\n{cm}")

    return cm


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
