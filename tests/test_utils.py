# -*- coding: utf-8 -*-
"""Testing the utils module"""
import logging
import math
import pytest
from cjio_dbexport import utils

log = logging.getLogger(__name__)

class TestGeom:
    def test_read_geojson_polygon(self, nl_poly):
        polygon = utils.read_geojson_polygon(nl_poly)
        assert len(polygon) > 0

    def test_read_geojson_polygon_multi(self, nl_multi):
        with pytest.raises(ValueError):
            polygon = utils.read_geojson_polygon(nl_multi)

    def test_to_ewkt(self):
        polygon = [[(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]]
        expect = 'SRID=7415;POLYGON((0.0 0.0,1.0 1.0,1.0 0.0,0.0 0.0))'
        ewkt = utils.to_ewkt(polygon, srid=7415)
        assert ewkt == expect

    def test_to_ewkt_nl(self, nl_poly):
        polygon = utils.read_geojson_polygon(nl_poly)
        ewkt = utils.to_ewkt(polygon, srid=7415)
        log.debug(ewkt)

    def test_to_ewkt_grid(self, nl_poly):
        bbox = (1032.05, 286175.81, 304847.26, 624077.50)
        grid = utils.create_rectangle_grid_morton(bbox=bbox, hspacing=10000,
                                                  vspacing=10000)
        for code, poly in grid.items():
            ewkt = utils.to_ewkt(poly, srid=7415)
            log.debug(ewkt)

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
        lvls = math.log(len(grid), 4)
        assert lvls.is_integer()

    def test_index_quadtree(self):
        bbox = (1032.05, 286175.81, 304847.26, 624077.50)
        grid = utils.create_rectangle_grid_morton(bbox=bbox, hspacing=10000,
                                                  vspacing=10000)
        utils.index_quadtree(grid)
        log.debug("bla")

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

class TestParsing:
    @pytest.mark.parametrize('lod_num, lod_str',[
        (1, '1'),
        (0.3, '0.3'),
        (1.2, '1.2'),
        (0, '0'),
        (1.0, '1.0')
    ])
    def test_lod_to_string(self, lod_num, lod_str):
        assert utils.lod_to_string(lod_num) == lod_str

    @pytest.mark.parametrize('lod_key, lod_str', [
        ('lod0', '0'),
        ('lod03', '0.3'),
        ('lod1', '1'),
        ('lod13', '1.3')
    ])
    def test_lod_param(self, lod_key, lod_str):
        assert utils.parse_lod_value(lod_key) == lod_str