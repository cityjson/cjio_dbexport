# -*- coding: utf-8 -*-
"""Testing the utils module"""
import logging
from cjio_dbexport import utils

log = logging.getLogger(__name__)

class TestUtils:
    def test_create_rectangle_grid(self):
        bbox = (1032.05, 286175.81, 304847.26, 624077.50)
        grid = utils.create_rectangle_grid(bbox=bbox, hspacing=10000,
                                           vspacing=10000)
        assert len(grid) == 1054