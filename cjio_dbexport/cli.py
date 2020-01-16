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

import click
from cjio import cityjson

from cjio_dbexport import recorder, configure, db, db3dnl


@click.group()
@click.option(
    '--verbose', '-v',
    count=True,
    help="Increase verbosity. You can increment the level by chaining the "
         "argument, eg. -vvv")
@click.option(
    '--quiet', '-q',
    count=True,
    help="Decrease verbosity.")
@click.argument('configuration', type=click.File('r'))
@click.pass_context
def main(ctx, verbose, quiet, configuration):
    """Export tool from PostGIS to CityJSON.

    CONFIGURATION is the YAML configuration file.
    """
    ctx.ensure_object(dict)
    verbosity = verbose - quiet
    recorder.configure_logging(verbosity)
    # For logging from the click commands
    ctx.obj['log'] = logging.getLogger(__name__)
    ctx.obj['cfg'] = configure.parse_configuration(configuration)
    return 0


@click.command('export')
@click.option('--bbox', nargs=4, type=float,
              help='2D bbox: (minx miny maxx maxy).')
@click.argument('filename', type=str)
@click.pass_context
def export_cmd(ctx, bbox, filename):
    """Export into a CityJSON file."""
    path = Path(filename).resolve()
    if not Path(path.parent).exists():
        raise NotADirectoryError(f"Directory {path.parent} not exists")
    conn = db.Db(**ctx.obj['cfg']['database'])
    try:
        cm = db3dnl.export(conn=conn,
                           cfg=ctx.obj['cfg'],
                           bbox=bbox)
        cityjson.save(cm, path=path, indent=None)
    except Exception as e:
        raise click.exceptions.ClickException(e)
    finally:
        conn.close()


@click.command('index')
@click.argument('extent', type=click.File('r'))
@click.argument('tilesize', type=float, nargs=2)
@click.pass_context
def index_cmd(ctx, extent, tilesize):
    """Create a tile index for the specified extent.

    Run this command to create rectangular tiles for EXTENT and store the
    resulting tile index in the database. The size of the tiles is set by
    TILESIZE X-width Y-width.

    EXTENT is a GeoJSON file that contains a single Polygon. For example if you
    want to create a tile index for the Netherlands, EXTENT would be the
    polygon boundary of the Netherlands.

    For example the command below will,

    (1) create rectangular polygons (tiles) of 1000m by 1000m for the extent
        of the polygon that is 'netherlands.json',

    (2) sort the tiles in Morton-order and create unique IDs for them
        accordingly,

    (3) upload the tile index into the relation that is declared in 'config.yml'
    under the 'tile_index' node.

        $ cjdb config.yml index netherlands.json 1000 1000
    """


main.add_command(export_cmd)
main.add_command(index_cmd)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
