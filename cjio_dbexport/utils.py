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
import math
from typing import Iterable

def create_rectangle_grid(bbox: Iterable[float], hspacing: float, vspacing: float) -> Iterable:
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