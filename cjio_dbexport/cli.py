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
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from psycopg2 import Error as pgError
from psycopg2 import sql
import click
from cjio import cityjson

import cjio_dbexport.utils
from cjio_dbexport import recorder, configure, db, db3dnl, tiler, utils


@click.group()
@click.option(
    '--log',
    type=click.Choice(
        ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        case_sensitive=False),
    default='DEBUG',
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
    conn = db.Db(**ctx.obj['cfg']['database'])
    try:
        click.echo(f"Exporting the whole database")
        cm = db3dnl.export(conn=conn,
                           cfg=ctx.obj['cfg'])
        cityjson.save(cm, path=path, indent=None)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)
    finally:
        conn.close()

@click.command('export_tiles')
@click.argument('tiles', nargs=-1, type=str)
@click.option('--merge', is_flag=True,
              help='Merge the requested tiles into a single file')
@click.option('--threads', type=int,
              help='Merge the requested tiles into a single file')
@click.argument('dir', type=str)
@click.pass_context
def export_tiles_cmd(ctx, tiles, merge, threads, dir):
    """Export the objects within the given tiles into a CityJSON file.

    TILES is a list of tile IDs from the tile_index, or 'all' which exports
    the object from all tiles from the tile_index.

    DIR is the path to the output directory.
    """
    log = ctx.obj['log']
    path = Path(dir).resolve()
    if not Path(path.parent).exists():
        raise NotADirectoryError(f"Directory {path.parent} not exists")
    conn = db.Db(**ctx.obj['cfg']['database'])
    tile_index = db.Schema(ctx.obj['cfg']['tile_index'])
    try:
        tile_list = db3dnl.with_list(conn=conn, tile_index=tile_index,
                                     tile_list=tiles)
        click.echo(f"Found {len(tile_list)} tiles in the tile index.")
    except BaseException as e:
        raise click.ClickException(f"Could not generate tile_list. Check the "
                                   f"logs for details.\n{e}")
    finally:
        conn.close()

    if merge:
        filepath = (path / 'merged').with_suffix('.json')
        try:
            click.echo(f"Exporting merged tiles {tiles}")
            dbexport = db3dnl.export(
                conn_cfg=ctx.obj['cfg']['database'],
                tile_index=ctx.obj['cfg']['tile_index'],
                cityobject_type=ctx.obj['cfg']['cityobject_type'],
                tile_list=tile_list)
            cm = db3dnl.convert(dbexport)
            cityjson.save(cm, path=filepath, indent=None)
            click.echo(f"Saved merged CityJSON tiles to {filepath}")
        except BaseException as e:
            raise click.ClickException(e)
    else:
        click.echo(f"Exporting {len(tile_list)} tiles...")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_export = {}
            failed = []
            for tile in tile_list:
                filepath = (path / str(tile)).with_suffix('.json')
                try:
                    dbexport = db3dnl.export(
                        conn_cfg=ctx.obj['cfg']['database'],
                        tile_index=ctx.obj['cfg']['tile_index'],
                        cityobject_type=ctx.obj['cfg']['cityobject_type'],
                        tile_list=(tile,))
                except BaseException as e:
                    log.error(f"Failed to export tile {str(tile)}\n{e}")
                future = executor.submit(db3dnl.to_citymodel, dbexport)
                future_to_export[future] = filepath
            for i,future in enumerate(as_completed(future_to_export)):
                filepath = future_to_export[future]
                cm = future.result()
                if cm is not None:
                    try:
                        with open(filepath, 'w') as fout:
                            json_str = json.dumps(cm.j, indent=None)
                            fout.write(json_str)
                        click.echo(f"[{i+1}/{len(tile_list)}] Saved CityJSON to {filepath}")
                    except IOError as e:
                        log.error(f"Invalid output file: {filepath}\n{e}")
                        failed.append(filepath.stem)
                else:
                    click.echo(f"Failed to create CityJSON from {filepath.stem},"
                               f" check the logs for details.")
                    failed.append(filepath.stem)
                del future_to_export[future]
                del cm
        click.echo(f"Done. Exported {len(tile_list) - len(failed)} tiles. "
                   f"Failed {len(failed)} tiles: {failed}")


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
    conn = db.Db(**ctx.obj['cfg']['database'])
    try:
        cm = db3dnl.export(conn=conn,
                           cfg=ctx.obj['cfg'],
                           bbox=bbox)
        cityjson.save(cm, path=path, indent=None)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)
    finally:
        conn.close()


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
    conn = db.Db(**ctx.obj['cfg']['database'])
    try:
        cm = db3dnl.export(conn=conn,
                           cfg=ctx.obj['cfg'],
                           extent=polygon)
        cityjson.save(cm, path=path, indent=None)
        click.echo(f"Saved CityJSON to {path}")
    except Exception as e:
        raise click.exceptions.ClickException(e)
    finally:
        conn.close()


@click.command('index')
@click.argument('extent', type=click.File('r'))
@click.argument('tilesize', type=float, nargs=2)
@click.option('--drop', is_flag=True,
              help="Drop the tile_index.table if it exists.")
@click.pass_context
def index_cmd(ctx, extent, tilesize, drop):
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
    conn = db.Db(**ctx.obj['cfg']['database'])
    try:
        tile_index = db.Schema(ctx.obj['cfg']['tile_index'])
        pgversion = conn.check_postgis()
        if pgversion is None:
            raise click.ClickException(
                f"PostGIS is not installed in {conn.dbname}")
        else:
            log.debug(f"PostGIS version={pgversion}")
        # Upload the extent to a temporary table
        extent_tbl = sql.Identifier('extent')
        good = tiler.create_temp_table(conn=conn,
                                       srid=ctx.obj['cfg']['tile_index']['srid'],
                                       extent=extent_tbl)
        if not good:
            raise click.ClickException(f"Could not create TEMPORARY TABLE for "
                                       f"the extent. Check the logs for "
                                       f"details.")
        extent_ewkt = utils.to_ewkt(polygon=polygon,
                                    srid=ctx.obj['cfg']['tile_index']['srid'])
        good = tiler.insert_ewkt(conn=conn,
                                 temp_table=extent_tbl,
                                 ewkt=extent_ewkt)
        if not good:
            raise click.ClickException(f"Could not insert the extent into the "
                                       f" 'extent' temporary table. Check the "
                                       f"logs for details.")
        # Create tile_index table
        table = (tile_index.schema + tile_index.table).as_string(conn.conn)
        good = tiler.create_tx_table(conn, tile_index=tile_index,
                                     srid=ctx.obj['cfg']['tile_index']['srid'],
                                     drop=drop)
        if good:
            click.echo(f"Created {table} in {conn.dbname}")
        else:
            raise click.ClickException(
                f"Could not create {tile_index.schema.string}."
                f"{tile_index.table.string} in {conn.dbname}. Check the logs for "
                f"details.")
        # Upload the tile_index to the database
        values = StringIO()
        for idx, code in quadtree_idx.items():
            ewkt = utils.to_ewkt(polygon=grid[code],
                                 srid=ctx.obj['cfg']['tile_index']['srid'])
            values.write(f'{idx}\t{ewkt}\n')
        values.seek(0)
        log.debug(f"First <value>={values.readline()}")
        values.seek(0)
        try:
            with conn.conn:
                with conn.conn.cursor() as cur:
                    log.debug(f"COPY {table} ({tile_index.field.pk.string},"
                              f" {tile_index.field.geometry.string}) FROM "
                              f"<value> ")
                    cur.copy_from(values, table,
                                  columns=(tile_index.field.pk.string,
                                           tile_index.field.geometry.string),
                                  sep='\t')
            click.echo(f"Inserted {len(grid)} tiles into {table}")
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
                f"Could not create SP-GiST on {table} geometry."
                f"Check the logs for details.")
    finally:
        conn.close()


main.add_command(export_all_cmd)
main.add_command(export_bbox_cmd)
main.add_command(export_extent_cmd)
main.add_command(export_tiles_cmd)

main.add_command(index_cmd)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
