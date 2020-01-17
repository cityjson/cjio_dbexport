# -*- coding: utf-8 -*-
"""Various utility functions.

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
import json
import math
from statistics import mean
from typing import Iterable, Tuple, Mapping, TextIO
import logging

log = logging.getLogger(__name__)

def create_rectangle_grid(bbox: Iterable[float], hspacing: float,
                          vspacing: float) -> Iterable:
    """

    :param bbox: (xmin, ymin, xmax, ymax)
    :param hspacing:
    :param vspacing:
    :return: A MultiPolygon of rectangular polygons (the grid) as Simple Feature
    """
    xmin, ymin, xmax, ymax = bbox
    width = math.ceil(xmax - xmin)
    height = math.ceil(ymax - ymin)
    cols = math.ceil(width / hspacing)
    rows = math.ceil(height / vspacing)
    multipolygon = list()

    for col in range(cols):
        x1 = float(xmin) + (col * hspacing)
        x2 = x1 + hspacing

        for row in range(rows):
            y1 = float(ymax) - (row * vspacing)
            y2 = y1 - vspacing

            # A polygon with a single (outer) ring
            polygon = [[(x1, y1), (x1, y2), (x2, y2), (x2, y1), (x1, y1)]]
            multipolygon.append(polygon)

    return multipolygon

def create_rectangle_grid_morton(bbox: Iterable[float], hspacing: float,
                                 vspacing: float) -> Mapping:
    """Creates a grid of rectangular polygons and computes their Morton code.

    If the width or height of the ``bbox`` is not divisible by 4 without a
    remainder, then the extent of the grid is expanded until it is. The reason
    for doing so is that the output grid is meant to be used as the leafs of a
    quadtree.

    :param bbox: (xmin, ymin, xmax, ymax)
    :param hspacing: Width of a cell
    :param vspacing: Height of a cell
    :return: A dictionary of {morton code: Polygon}. Polygon is represented as
        Simple Feature.
    """
    xmin, ymin, xmax, ymax = bbox
    width = math.ceil(xmax - xmin)
    height = math.ceil(ymax - ymin)
    _c = math.ceil(width / hspacing)
    cols = _c + (_c % 4)
    _r = math.ceil(height / vspacing)
    rows = _r + (_r % 4)
    if rows < cols:
        rows += cols - rows
    elif cols < rows:
        cols += rows - cols
    # Expand extent until we get enough cells for a full quadtree
    exponent = math.ceil(math.log(rows*cols, 4))
    full_cells = 4**exponent
    rows = int(math.sqrt(full_cells))
    cols = int(math.sqrt(full_cells))
    grid = dict()


    for col in range(cols):
        x1 = float(xmin) + (col * hspacing)
        x2 = x1 + hspacing

        for row in range(rows):
            y1 = float(ymax) - (row * vspacing)
            y2 = y1 - vspacing

            ring = [(x1, y1), (x1, y2), (x2, y2), (x2, y1), (x1, y1)]
            # A polygon with a single (outer) ring
            polygon = [ring,]

            centroid = mean_coordinate(ring)
            morton_key = morton_code(*centroid)

            grid[morton_key] = polygon

    return dict((k, grid[k]) for k in sorted(grid))


def index_quadtree(grid):
    """Create indices for the leafs of the quadtree.

    Based on AHN's tile indexing.

    :param grid: A rectangular grid of polygons which has 4**x cells. The cells
        must be sorted in Morton-order.
    """
    quadtree = dict()
    nr_cells = len(grid)
    if not math.log(nr_cells, 4).is_integer():
        raise ValueError(f"There are {nr_cells} in the grid. The grid must "
                         f"contain 4**x cells to form a full quadtree. ")
    # Nr. levels in the quadtree
    nr_lvls = int(math.log(nr_cells, 4))
    log.debug(f"Nr. levels={nr_lvls}, cells={nr_cells}")

    id_map = {
        0 : ('1', '2', '3', '4'),
        1 : ('a', 'b', 'c', 'd'),
        2 : ('e', 'f', 'g', 'i'),
        3 : ('1', '2', '3', '4'),
        4 : ('1', '2', '3', '4')
    }

    # Extend the ID map to have as many levels as there are in the quadtree
    if nr_lvls > len(id_map):
        diff = nr_lvls - len(id_map)
        for i in range(diff):
            id_map[5+i] = id_map[i]

    # Compose the cell IDs per level
    for i, mcode in enumerate(grid):
        cell_id = ""
        for j in range(nr_lvls, 0, -1):
            lvl_id = id_map[j-1]
            lvl_idx = int((i % 4**j) / 4**(j-1))
            cell_id += lvl_id[lvl_idx]
        if cell_id in quadtree:
            raise IndexError(f"ID {cell_id} already exists in the quadtree")
        else:
            quadtree[cell_id] = mcode

    return quadtree


def bbox(polygon: Iterable) -> Tuple[float, float, float, float]:
    """Compute the Bounding Box of a polygon.

    :param polygon: A Simple Feature Polygon, defined as [[[x1, y1], ...], ...]
    """
    x,y = 0,1
    vtx = polygon[0][0]
    minx, miny, maxx, maxy = vtx[x], vtx[y], vtx[x], vtx[y]
    for ring in polygon:
        for vtx in ring:
            if vtx[x] < minx:
                minx = vtx[x]
            elif vtx[y] < miny:
                miny = vtx[y]
            elif vtx[x] > maxx:
                maxx = vtx[x]
            elif vtx[y] > maxy:
                maxy = vtx[y]
    return minx, miny, maxx, maxy


def mean_coordinate(points: Iterable[Tuple]) -> Tuple[float, float]:
    """Compute the mean x- and y-coordinate from a list of points.

    :param points: An iterable of coordinate tuples where the first two elements
        of the tuple are the x- and y-coordinate respectively.
    :returns: A tuple of (mean x, mean y) coordinates
    """
    mean_x = mean(pt[0] for pt in points)
    mean_y = mean(pt[1] for pt in points)
    return mean_x, mean_y

# Computing Morton-code. Reference: https://github.com/trevorprater/pymorton ---

def __part1by1_64(n):
    """64-bit mask"""
    n &= 0x00000000ffffffff                  # binary: 11111111111111111111111111111111,                                len: 32
    n = (n | (n << 16)) & 0x0000FFFF0000FFFF # binary: 1111111111111111000000001111111111111111,                        len: 40
    n = (n | (n << 8))  & 0x00FF00FF00FF00FF # binary: 11111111000000001111111100000000111111110000000011111111,        len: 56
    n = (n | (n << 4))  & 0x0F0F0F0F0F0F0F0F # binary: 111100001111000011110000111100001111000011110000111100001111,    len: 60
    n = (n | (n << 2))  & 0x3333333333333333 # binary: 11001100110011001100110011001100110011001100110011001100110011,  len: 62
    n = (n | (n << 1))  & 0x5555555555555555 # binary: 101010101010101010101010101010101010101010101010101010101010101, len: 63

    return n


def __unpart1by1_64(n):
    n &= 0x5555555555555555                  # binary: 101010101010101010101010101010101010101010101010101010101010101, len: 63
    n = (n ^ (n >> 1))  & 0x3333333333333333 # binary: 11001100110011001100110011001100110011001100110011001100110011,  len: 62
    n = (n ^ (n >> 2))  & 0x0f0f0f0f0f0f0f0f # binary: 111100001111000011110000111100001111000011110000111100001111,    len: 60
    n = (n ^ (n >> 4))  & 0x00ff00ff00ff00ff # binary: 11111111000000001111111100000000111111110000000011111111,        len: 56
    n = (n ^ (n >> 8))  & 0x0000ffff0000ffff # binary: 1111111111111111000000001111111111111111,                        len: 40
    n = (n ^ (n >> 16)) & 0x00000000ffffffff # binary: 11111111111111111111111111111111,                                len: 32
    return n


def interleave(*args):
    """Interleave two integers"""
    if len(args) != 2:
        raise ValueError('Usage: interleave2(x, y)')
    for arg in args:
        if not isinstance(arg, int):
            print('Usage: interleave2(x, y)')
            raise ValueError("Supplied arguments contain a non-integer!")

    return __part1by1_64(args[0]) | (__part1by1_64(args[1]) << 1)


def deinterleave(n):
    if not isinstance(n, int):
        print('Usage: deinterleave2(n)')
        raise ValueError("Supplied arguments contain a non-integer!")

    return __unpart1by1_64(n), __unpart1by1_64(n >> 1)


def morton_code(x: float, y: float):
    """Takes an (x,y) coordinate tuple and computes their Morton-key.

    Casts float to integers by multiplying them with 100 (millimeter precision).
    """
    return interleave(int(x * 100), int(y * 100))


def rev_morton_code(morton_key: int) -> Tuple[float, float]:
    """Get the coordinates from a Morton-key"""
    x,y = deinterleave(morton_key)
    return float(x)/100.0, float(y)/100.0


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