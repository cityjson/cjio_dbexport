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

from cjio import cityjson
from cjio.models import CityObject, Geometry
from psycopg2 import sql

from cjio_dbexport import db


log = logging.getLogger(__name__)


def export(conn, cfg: Mapping, cotype: str, bbox=None):
    """Export a table to CityModel

    :param conn:
    :param cfg:
    :param cotype: CityObject type
    :param bbox:
    :return: A citymodel of :py:class:`cityjson.CityJSON`
    """
    # Get the features
    features = db.Schema(cfg['features'])

    # Exclude columns from the selection
    table_fields = conn.get_fields(features.schema + features.table)
    attr_select = sql.SQL(', ').join(sql.Identifier(col) for col in table_fields
                                      if col != features.field.pk.string and
                                      col != features.field.geometry.string and
                                      col != features.field.cityobject_id.string and
                                      col not in cfg['features']['field']['exclude'])
    # BBOX clause
    if bbox:
        log.info(f"Exporting with BBOX {bbox}")
        epsg = 7415
        where_bbox = sql.SQL(f"WHERE ST_Intersects({features.field.geometry.string},"
                             f"ST_MakeEnvelope({','.join(map(str, bbox))}, {str(epsg)}))")
    else:
        where_bbox = sql.SQL("")

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
    resultset = conn.get_dict(query)

    cm = cityjson.CityJSON()

    # Loop through the whole resultset and create the CityObjects
    for record in resultset:
        # I expect this order of fields:
        #   0 - cityobject ID
        #   1 - geometry
        #   2 < attributes
        co = CityObject(id=record['coid'])

        # Parse the geometry
        geom = Geometry(type='Solid', lod=1)
        solid = []
        outer_shell = []
        for wkt_polyz in record['geom']:
            surface = parse_polygonz(wkt_polyz)
            # OPTIMIZE: make use of the generator down the line
            outer_shell.append(list(surface))
        solid.append(outer_shell)
        geom.boundaries = solid
        co.geometry.append(geom)

        # Parse attributes
        co.attributes = {key: attr for key, attr in record.items()
                         if key != 'pk' and key != 'geom' and key != 'coid'}

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
