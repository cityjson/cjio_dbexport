# -*- coding: utf-8 -*-
"""Console script for cjio_dbexport.

Copyright(C) 2019, 3D geoinformation group, Delft University of Technology.
All rights reserved.

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
import sys
from pathlib import Path
from io import StringIO
from multiprocessing import freeze_support
import json

import psycopg
from psycopg.errors import Error as pgError
from psycopg import sql
import click
from cjio import cityjson

import pgutils

import cjio_dbexport.utils
from cjio_dbexport import recorder, configure, db3dnl, tiler, utils, __version__


def save(cm: cityjson.CityJSON, path: Path, indent=False):
    """Write a CityJSON object to a JSON file.

    We need this function because cjio.cityjson.save() is deprecated with v0.8.0.
    """
    try:
        with path.open("w") as fout:
            if indent:
                json_str = json.dumps(cm.j, indent="\t")
            else:
                json_str = json.dumps(cm.j, separators=(',',':'))
            fout.write(json_str)
    except IOError as e:
        raise IOError('Invalid output file: %s \n%s' % (path, e))


@click.group()
@click.version_option(version=__version__)
@click.option(
    '--log',
    type=click.Choice(
        ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        case_sensitive=False),
    default='INFO',
    help="Set the logging level in the log file 'cjdb.log'.")
@click.argument('configuration', type=click.File('r'))
@click.pass_context
def main(ctx, log, configuration):
    """Export tool from PostGIS to CityJSON.

    CONFIGURATION is the YAML configuration file.
    """
    ctx.ensure_object(dict)
    logfile = 'cjdb.log'
    recorder.configure_logging(log, logfile)
    click.echo(f"Writing logs to {logfile}")
    # For logging from the click commands
    ctx.obj['log'] = logging.getLogger(__name__)
    ctx.obj['cfg'] = configure.parse_configuration(configuration)
    return 0


@click.command('export')
@click.argument('filename', type=str)
@click.pass_context
def export_all_cmd(ctx, filename):
    """Export the whole database into a CityJSON file.

    FILENAME is the path and name of the output file.
    """
    path = Path(filename).resolve()
    if not Path(path.parent).exists():
        raise NotADirectoryError(f"Directory {path.parent} not exists")
    conn = pgutils.PostgresConnection(**ctx.obj['cfg']['database'])
    if not utils.create_functions(conn):
        raise click.exceptions.ClickException(
            "Could not create the required functions in PostgreSQL, "
            "check the logs for details")
    try:
        click.echo(f"Exporting the whole database")
        dbexport = db3dnl.query(conn_cfg=ctx.obj['cfg']['database'],
                                tile_index=ctx.obj['cfg']['tile_index'],
                                cityobject_type=ctx.obj['cfg'][
                                    'cityobject_type'], threads=1)
        cm = db3dnl.convert(dbexport, cfg=ctx.obj['cfg'])
        cm.j["metadata"]["fileIdentifier"] = path.name
        save(cm, path=path, indent=False)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)


@click.command('export_tiles')
@click.option('--merge', is_flag=True,
              help='Merge the requested tiles into a single file')
@click.option('--zip', is_flag=True,
              help='Zip the output file. On Linux and MacOS its Gzip.')
@click.option('--jobs', '-j', type=int, default=1,
              help='The number of parallel jobs to run')
@click.option("--features", is_flag=True, help="Export CityJSONFeatures.")
@click.argument('tiles', nargs=-1, type=str)
@click.argument('dir', type=str)
@click.pass_context
def export_tiles_cmd(ctx, tiles: tuple[str, ...], merge: bool, zip: bool, jobs: int, features: bool, dir: str):
    """Export the objects within the given tiles into a CityJSON file.

    TILES is a list of tile IDs from the tile_index, or 'all' which exports
    the object from all tiles from the tile_index.

    DIR is the path to the output directory. It will be created if doesn't exist.

    When exporting to CityJSONFeatures, a directory tree is created from the tile IDs,
    and each tile directory contains the features in that tile. Each feature is written
    to a separate file.
    At the root of the directory tree the 'metadata.city.json' file is written, which
    contains the CRS and transformation properties for all the features.
    """
    path = Path(dir).resolve()
    path.mkdir(parents=True, exist_ok=True)
    # Need to have a list instead of a tuple, because psycopg's Postgres array adaptor
    #  works with lists, but not with tuples.
    tiles_list = list(tiles)
    tile_list = db3dnl.get_tile_list(ctx.obj["cfg"], tiles_list)

    if merge:
        filepath = (path / 'merged').with_suffix('.json')
        try:
            click.echo(f"Exporting merged tiles {tiles}")
            dbexport = db3dnl.query(conn_cfg=ctx.obj['cfg']['database'],
                                    tile_index=ctx.obj['cfg']['tile_index'],
                                    cityobject_type=ctx.obj['cfg'][
                                        'cityobject_type'], threads=1,
                                    tile_list=tile_list)
            cm = db3dnl.convert(dbexport, cfg=ctx.obj['cfg'])
            cm.j["metadata"]["fileIdentifier"] = filepath.name
            save(cm, path=filepath, indent=False)
            click.echo(f"Saved merged CityJSON tiles to {filepath}")
        except BaseException as e:
            raise click.ClickException(e)
        return 0
    else:
        click.echo(f"Exporting {len(tile_list)} tiles...")
        click.echo(f"Output directory: {path}")
        db3dnl.export_tiles_multiprocess(ctx.obj['cfg'], jobs, path, tile_list,
                                         zip=zip, features=features)
        return 0


@click.command('export_bbox')
@click.argument('bbox', nargs=4, type=float)
@click.argument('filename', type=str)
@click.pass_context
def export_bbox_cmd(ctx, bbox, filename):
    """Export the objects within a 2D Bounding Box into a CityJSON file.

    BBOX is a 2D Bounding Box (minx miny maxx maxy). The units of the
    coordinates must match the CRS in the database.

    FILENAME is the path and name of the output file.
    """
    path = Path(filename).resolve()
    if not Path(path.parent).exists():
        raise NotADirectoryError(f"Directory {path.parent} not exists")
    conn = pgutils.PostgresConnection(**ctx.obj['cfg']['database'])
    if not utils.create_functions(conn):
        raise click.exceptions.ClickException("Could not create the required functions in PostgreSQL, check the logs for details")
    try:
        click.echo(f"Exporting with BBOX={bbox}")
        dbexport = db3dnl.query(conn_cfg=ctx.obj['cfg']['database'],
                                tile_index=ctx.obj['cfg']['tile_index'],
                                cityobject_type=ctx.obj['cfg'][
                                    'cityobject_type'], threads=1, bbox=bbox)
        cm = db3dnl.convert(dbexport, cfg=ctx.obj['cfg'])
        cm.j["metadata"]["fileIdentifier"] = path.name
        save(cm, path=path, indent=False)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)


@click.command('export_extent')
@click.argument('extent', type=click.File('r'))
@click.argument('filename', type=str)
@click.pass_context
def export_extent_cmd(ctx, extent, filename):
    """Export the objects within the given polygon into a CityJSON file.

    EXTENT is a GeoJSON file that contains a single Polygon. The CRS of the
    file must be data same as in the database.

    FILENAME is the path and name of the output file.
    """
    path = Path(filename).resolve()
    if not Path(path.parent).exists():
        raise NotADirectoryError(f"Directory {path.parent} not exists")

    polygon = cjio_dbexport.utils.read_geojson_polygon(extent)
    conn = pgutils.PostgresConnection(**ctx.obj['cfg']['database'])
    if not utils.create_functions(conn):
        raise click.exceptions.ClickException("Could not create the required functions in PostgreSQL, check the logs for details")
    try:
        click.echo(f"Exporting with polygonal selection. Polygon={extent.name}")
        dbexport = db3dnl.query(conn_cfg=ctx.obj['cfg']['database'],
                                tile_index=ctx.obj['cfg']['tile_index'],
                                cityobject_type=ctx.obj['cfg'][
                                    'cityobject_type'], threads=1, extent=polygon)
        cm = db3dnl.convert(dbexport, cfg=ctx.obj['cfg'])
        cm.j["metadata"]["fileIdentifier"] = path.name
        save(cm, path=path, indent=False)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)


@click.command('index')
@click.option('--drop', is_flag=True,
              help="Drop the tile_index.table if it exists.")
@click.option('--centroid', is_flag=True,
              help="Create a spatial index on the input geometry centroids.")
@click.argument('extent', type=click.File('r'))
@click.argument('tilesize', type=float, nargs=2)
@click.pass_context
def index_cmd(ctx, extent, tilesize, drop, centroid):
    """Create a tile index for the specified extent.

    Run this command to create rectangular tiles for EXTENT and store the
    resulting tile index in the database. The size of the tiles is set by
    TILESIZE width height.

    EXTENT is a GeoJSON file that contains a single Polygon. For example if you
    want to create a tile index for the Netherlands, EXTENT would be the
    polygon boundary of the Netherlands.

    Note that the CRS must be consistent in the EXTENT and in the tile_index
    table.

    For example the command below will,

    (1) create rectangular polygons (tiles) of 1000m by 1000m for the extent
        of the polygon that is 'netherlands.json',

    (2) sort the tiles in Morton-order and create unique IDs for them
        accordingly,

    (3) upload the tile index into the relation that is declared in 'config.yml'
    under the 'tile_index' node.

        $ cjdb config.yml index netherlands.json 1000 1000
    """
    log = ctx.obj['log']
    polygon = cjio_dbexport.utils.read_geojson_polygon(extent)
    bbox = utils.bbox(polygon)
    log.debug(f"BBOX {bbox}")
    click.echo(f"Tilesize is set to width={tilesize[0]}, height={tilesize[1]}"
               f" in CRS units")
    # Create a rectangular grid of 4**x cells
    grid = utils.create_rectangle_grid_morton(bbox=bbox, hspacing=tilesize[0],
                                              vspacing=tilesize[1])
    click.echo(f"Created {len(grid)} tiles")
    # Create the IDs for the tiles
    quadtree_idx = utils.index_quadtree(grid)
    # Check if schema and table exists
    conn = pgutils.PostgresConnection(**ctx.obj['cfg']['database'])
    try:
        tile_index = pgutils.Schema(ctx.obj['cfg']['tile_index'])
        pgversion = conn.check_postgis()
        if pgversion is None:
            raise click.ClickException(
                f"PostGIS is not installed in {conn.dbname}")
        else:
            log.debug(f"PostGIS version={pgversion}")
        # Upload the extent to a temporary table
        extent_tbl = sql.Identifier('cjio_dbexport_extent')
        good = tiler.create_extent_table(conn=conn,
                                         srid=ctx.obj['cfg']['tile_index']['srid'],
                                         extent=extent_tbl)
        if not good:
            raise click.ClickException(f"Could not create TABLE for "
                                       f"the extent. Check the logs for "
                                       f"details.")
        extent_ewkt = utils.polygon_to_ewkt(polygon=polygon,
                                            srid=ctx.obj['cfg']['tile_index']['srid'])
        good = tiler.insert_ewkt(conn=conn, extent_table=extent_tbl, ewkt=extent_ewkt)
        if not good:
            raise click.ClickException(f"Could not insert the extent into the "
                                       f" 'cijo_dbexport_extent' table. Check the "
                                       f"logs for details.")
        # Create tile_index table
        table = (tile_index.schema + tile_index.table).as_string()
        good = tiler.create_tx_table(conn, tile_index=tile_index,
                                     srid=ctx.obj['cfg']['tile_index']['srid'],
                                     drop=drop)
        if good:
            click.echo(f"Created {table} in {conn.dbname}")
        else:
            raise click.ClickException(
                f"Could not create {tile_index.schema}."
                f"{tile_index.table} in {conn.dbname}. Check the logs for "
                f"details.")
        
        # Upload the tile_index to the database
        for idx, code in quadtree_idx.items():
            values = StringIO()
            sw_boundary = utils.rectangle_sw_boundary(grid[code])
            ewkt = utils.polygon_to_ewkt(polygon=grid[code],
                                         srid=ctx.obj['cfg']['tile_index']['srid'])
            ewkt_sw = utils.polyline_to_ewkt(sw_boundary,
                                             srid=ctx.obj['cfg']['tile_index']['srid'])
            values.write(f'{idx}\t{ewkt}\t{ewkt_sw}\n')
            values.seek(0)
            try:
                with conn.connect() as connection:
                    with connection.cursor() as cur:
                        query = f"COPY {table} ({tile_index.field.pk}, {tile_index.field.geometry}, {tile_index.field.geometry_sw_boundary}) FROM STDIN WITH DELIMITER '\t'"
                        log.debug(query)
                        with cur.copy(query) as copy:
                            while data := values.read():
                                copy.write(data)
                log.debug(f"Inserted {idx} tile into {table}")
            except pgError as e:
                raise click.ClickException(e)
            finally:
                values.close()

        # Clip the tile index with the extent
        click.echo(f"Clipping tile index {table} to the provided extent "
                   f"polygon")
        good = tiler.clip_grid(conn=conn,
                               tile_index=tile_index,
                               extent=extent_tbl)
        if not good:
            raise click.ClickException(
                f"Could not clip the tile index to the extent."
                f"Check the logs for details.")
        # Create spatial index on the tile index
        good = tiler.gist_on_grid(conn=conn,
                                  tile_index=tile_index)
        if not good:
            raise click.ClickException(
                f"Could not create GiST on {table} geometry."
                f"Check the logs for details.")
        if centroid:
            click.echo("Indexing input geometry centroids")
            good = db3dnl.index_geometry_centroid(conn, ctx.obj['cfg'])
        if not good:
            raise click.ClickException(
                f"Could not GiST on feature geometry centroids."
                f"Check the logs for details.")
    except Exception as e:
        raise click.ClickException(e)


main.add_command(export_all_cmd)
main.add_command(export_bbox_cmd)
main.add_command(export_extent_cmd)
main.add_command(export_tiles_cmd)

main.add_command(index_cmd)

if __name__ == "__main__":
    freeze_support()
    sys.exit(main())  # pragma: no cover
