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
from typing import Mapping
from datetime import datetime

from cjio import cityjson
from cjio.models import CityObject, Geometry
from psycopg2 import sql

from cjio_dbexport import db


log = logging.getLogger(__name__)

def build_query(conn: db.Db, features: db.Schema, bbox=None):
    """Build an SQL query for extracting CityObjects from a single table.

    ..todo: make EPSG a parameter

    :param conn:
    :param cfg:
    :param features:
    :param bbox:
    :return:
    """
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
    # BBOX clause
    if bbox:
        log.info(f"Exporting with BBOX {bbox}")
        epsg = 7415
        where_bbox = sql.SQL(f"WHERE ST_Intersects({features.field.geometry.string},"
                             f"ST_MakeEnvelope({','.join(map(str, bbox))}, {str(epsg)}))")
    else:
        where_bbox = sql.SQL("")

    # Main query
    query_params = {
        'pk': features.field.pk.sqlid,
        'coid': features.field.cityobject_id.sqlid,
        'geometry': features.field.geometry.sqlid,
        'table': features.schema + features.table,
        'attr': attr_select,
        'where_bbox': where_bbox
    }

    query = sql.SQL("""
    WITH attrs AS (
        SELECT
            {pk} pk,
            {attr}
        FROM
            {table}
    ),
    polygons AS (
        SELECT
            {pk} pk,
            (ST_Dump({geometry})).geom,
            {coid} coid
        FROM
            {table}
        {where_bbox}
    ),
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

    return query


def export(conn: db.Db, cfg: Mapping, bbox=None):
    """Export a table to CityModel

    :param conn:
    :param cfg:
    :param bbox:
    :return: A citymodel of :py:class:`cityjson.CityJSON`
    """
    cm = cityjson.CityJSON()
    for cotype, cotables in cfg['cityobject_type'].items():
        for cotable in cotables:
            log.info(f"CityObject {cotype} from table {cotable['table']}")
            if cotype.lower() == 'building':
                geomtype = 'Solid'
            else:
                # FIXME: because CompositeSurface is not supported at the moment for semantic surfaces in cjio.models
                geomtype = 'MultiSurface'

            features = db.Schema(cotable)

            query = build_query(conn=conn, features=features, bbox=bbox)
            resultset = conn.get_dict(query)

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
    cm.set_epsg(7415)
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
            yield pts[1:] # WKT repeats the first vertex
    else:
        log.error("Not a POLYGON Z")