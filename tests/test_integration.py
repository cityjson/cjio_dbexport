# -*- coding: utf-8 -*-
"""Integration testing."""

from pathlib import Path
from click.testing import CliRunner

from cjio_dbexport import cli, db3dnl, db

class TestIntegration:
    def test_export(self, data_dir, cfg, db3dnl_db):
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg, cotype='Building')

    def test_export_bbox(self, data_dir, cfg, db3dnl_db):
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg, cotype='Building',
                           bbox=[192837.734,465644.179,193701.818,466898.821])

