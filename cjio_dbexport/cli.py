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
import sys
import logging

import click

from cjio_dbexport import recorder,configure


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
@click.argument('output', type=click.File('w'))
@click.pass_context
def export_cmd(ctx, bbox, output):
    """Export into a CityJSON file."""


main.add_command(export_cmd)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
