# -*- coding: utf-8 -*-
"""Testing the 3DNL exporter"""

import logging
import pytest
from cjio_dbexport import db3dnl, db

@pytest.mark.db3dnl
def test_parse_boundary():
    resultset = (1, [
        'POLYGON Z ((194427.785999119 466111.837000076 25.2199993133545,194437.779999554 466112.182000075 25.2199993133545,194427.371 466123.86 25.2199993133545,194427.785999119 466111.837000076 25.2199993133545))',
        'POLYGON Z ((194437.364999562 466124.204999838 18.7099990844727,194437.779999554 466112.182000075 18.7099990844727,194427.371 466123.86 18.7099990844727,194437.364999562 466124.204999838 18.7099990844727))',
        'POLYGON Z ((194427.785999119 466111.837000076 25.2199993133545,194437.779999554 466112.182000075 25.2199993133545,194427.371 466123.86 25.2199993133545,194427.785999119 466111.837000076 25.2199993133545), (1 2 3, 3 4 5), (6 7 9, 3 4 5))'
    ])
    msurface = []
    for polyz in resultset[1]:
        surface = db3dnl.parse_polygonz(polyz)
        msurface.append(list(surface))
    print(msurface)

@pytest.mark.db3dnl
def test_build_query(db3dnl_db, cfg_parsed):
    features = db.Schema(cfg_parsed['cityobject_type']['LandUse'][0])
    query = db3dnl.build_query(conn=db3dnl_db, features=features,
                               bbox=[192837.734, 465644.179, 193701.818,
                                     466898.821])
    query_str = db3dnl_db.print_query(query)
    assert '"xml"' not in query_str

@pytest.mark.db3dnl
class TestIntegration:
    """Integration tests"""

    def test_export(self, data_dir, cfg_parsed, db3dnl_db):
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg_parsed)
        print(cm.get_info())

    def test_export_bbox(self, data_dir, cfg_parsed, db3dnl_db, caplog):
        caplog.set_level(logging.DEBUG)
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg_parsed,
                           bbox=[192837.734, 465644.179, 193701.818,
                                 466898.821])
        print(cm.get_info())
        # assert cm.validate()
