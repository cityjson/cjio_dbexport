# -*- coding: utf-8 -*-
"""Integration testing."""

from pathlib import Path

from cjio_dbexport import db3dnl

class TestIntegration:
    def test_export(self, data_dir, cfg_parsed, db3dnl_db):
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg_parsed, cotype='Building')
        print(cm.get_info())

    def test_export_bbox(self, data_dir, cfg_parsed, db3dnl_db):
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg_parsed, cotype='Building',
                           bbox=[192837.734,465644.179,193701.818,466898.821])
        print(cm.get_info())
        # assert cm.validate()

