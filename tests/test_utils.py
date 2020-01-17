# -*- coding: utf-8 -*-
"""Testing the utils module"""
import logging
import pytest
from cjio_dbexport import utils

log = logging.getLogger(__name__)


class TestBBOX:
    @pytest.mark.parametrize('polygon, bbox', [
        [[[(1.0, 4.0), (3.0,1.0), (6.0, 2.0), (6.0, 6.0), (2.0, 7.0)]], (1.0, 1.0, 6.0, 7.0)],
        [[[(1.0, 4.0), (3.0,1.0), (6.0, 2.0), (6.0, 6.0), (2.0, 7.0), (1.0, 4.0)]], (1.0, 1.0, 6.0, 7.0)]
    ])
    def test_bbox(self, polygon, bbox):
        assert utils.bbox(polygon) == bbox

class TestGrid:
    def test_create_rectangle_grid(self):
        bbox = (1032.05, 286175.81, 304847.26, 624077.50)
        grid = utils.create_rectangle_grid(bbox=bbox, hspacing=10000,
                                           vspacing=10000)
        assert len(grid) == 1054

    def test_create_rectangle_grid_morton(self):
        bbox = (1032.05, 286175.81, 304847.26, 624077.50)
        grid = utils.create_rectangle_grid_morton(bbox=bbox, hspacing=10000,
                                                  vspacing=10000)
        assert len(grid) == 1054

class TestSorting:
    @pytest.mark.parametrize('point', [
        (0, 0),
        (0.0, 0.0),
        (1.0, 1.0),
        (96663.25590546813, 439718.94288361823),
        (252914.232, 608211.603)
    ])
    def test_morton_code(self, point):
        utils.morton_code(*point)

    def test_rev_morton_code(self):
        point = (252914.232, 608211.603)
        morton_key = utils.morton_code(*point)
        point_res = utils.rev_morton_code(morton_key)
        assert pytest.approx(point[0], point_res[0]) and \
               pytest.approx(point[1], point_res[1])