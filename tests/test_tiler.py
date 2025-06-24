# -*- coding: utf-8 -*-
"""Testing the tiler module."""
from cjio_dbexport import tiler
import logging
from psycopg import sql
import pytest

log = logging.getLogger(__name__)


class TestTiler:
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(request, db3dnl_db):
        db3dnl_db.send_query("CREATE SCHEMA IF NOT EXISTS temp;")

        yield

        db3dnl_db.send_query("DROP SCHEMA IF EXISTS temp CASCADE;")

    def test_create_extent_table(self, db3dnl_db):
        assert tiler.create_extent_table(conn=db3dnl_db,
                                         srid=7415,
                                         extent=sql.Identifier('temp', 'extent'))
        res = db3dnl_db.get_query("select * from temp.extent;")
        log.info(res)

    def test_insert_ewkt(self, db3dnl_db):
        ewkt = 'SRID=7415;POLYGON((0.0 0.0, 1.0 1.0, 1.0 0.0, 0.0 0.0))'
        extent_table = sql.Identifier('temp', 'extent')
        assert tiler.insert_ewkt(conn=db3dnl_db, extent_table=extent_table, ewkt=ewkt)