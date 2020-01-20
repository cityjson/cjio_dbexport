# -*- coding: utf-8 -*-
"""Testing the tiler module."""
from cjio_dbexport import tiler
import logging
from psycopg2 import sql
import pytest

log = logging.getLogger(__name__)

@pytest.mark.cjdb
class TestTiler:
    def test_create_temp_table(self, cjdb_db):
        cfg = {'tile_index': {'srid': 7415}}
        assert tiler.create_temp_table(conn=cjdb_db,
                                       srid=7415,
                                       extent=sql.Identifier('extent'))
        res = cjdb_db.get_query("select * from extent;")
        log.info(res)

    def test_insert_ewkt(self, cjdb_db):
        ewkt = 'SRID=7415;POLYGON((0.0 0.0, 1.0 1.0, 1.0 0.0, 0.0 0.0))'
        temp_table = sql.Identifier('test_data', 'extent')
        assert tiler.insert_ewkt(conn=cjdb_db, temp_table=temp_table,
                                 ewkt=ewkt)