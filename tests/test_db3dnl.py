# -*- coding: utf-8 -*-
"""Testing the 3DNL exporter"""

import logging
import pytest
from pathlib import Path

import pickle

from cjio import cityjson
import cjio_dbexport.utils
from cjio_dbexport import db3dnl, db, tiler,utils

log = logging.getLogger(__name__)

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
    tile_index = db.Schema(cfg_parsed['tile_index'])
    query = db3dnl.build_query(conn=db3dnl_db, features=features,
                               tile_index=tile_index,
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

    def test_export_tile_list(self, data_dir, cfg_parsed, db3dnl_db, caplog,
                              db3dnl_4tiles_pickle):
        caplog.set_level(logging.DEBUG)
        export_gen = db3dnl.export(conn_cfg=cfg_parsed['database'],
                                   tile_index=db.Schema(
                                       cfg_parsed['tile_index']),
                                   cityobject_type=cfg_parsed[
                                       'cityobject_type'],
                                   tile_list=['gb2', 'ic1', 'ic2', 'ec4'])
        dbexport = list(export_gen)
        with open(db3dnl_4tiles_pickle, 'wb') as fo:
            pickle.dump(dbexport, fo)

    def test_export_no_pool_tile_list(self, data_dir, cfg_parsed, db3dnl_db, caplog,
                              db3dnl_4tiles_pickle):
        caplog.set_level(logging.DEBUG)
        export_gen = db3dnl.export_no_pool(conn_cfg=cfg_parsed['database'],
                                   tile_index=db.Schema(
                                       cfg_parsed['tile_index']),
                                   cityobject_type=cfg_parsed[
                                       'cityobject_type'],
                                   tile_list=['gb2', 'ic1', 'ic2', 'ec4'])
        dbexport = list(export_gen)
        with open(db3dnl_4tiles_pickle, 'wb') as fo:
            pickle.dump(dbexport, fo)

    def test_export_single_tile_list(self, data_dir, cfg_parsed, db3dnl_db,
                                     caplog, db3dnl_4tiles_pickle):
        caplog.set_level(logging.DEBUG)
        dbexport = list(db3dnl.export_single(conn=db3dnl_db, cfg=cfg_parsed,
                                      tile_list=['gb2', 'ic1', 'ic2',
                                                 'ec4']))
        with open(db3dnl_4tiles_pickle, 'wb') as fo:
            pickle.dump(dbexport, fo)

    def test_convert(self, data_dir, db3dnl_4tiles_pickle, caplog):
        caplog.set_level(logging.DEBUG)
        with open(db3dnl_4tiles_pickle, 'rb') as fo:
            dbexport = pickle.load(fo)
        cm = db3dnl.convert(dbexport)
        cm.get_info()

    def test_export_extent(self, data_dir, cfg_parsed, db3dnl_db, caplog):
        caplog.set_level(logging.DEBUG)
        with open(data_dir / 'db3dnl_poly.geojson', 'r') as fo:
            polygon = utils.read_geojson_polygon(fo)
        cm = db3dnl.export(conn=db3dnl_db, cfg=cfg_parsed,
                           extent=polygon)
        print(cm.get_info())

    def test_index(self, data_dir, nl_poly):
        tilesize = (10000, 10000)
        polygon = cjio_dbexport.utils.read_geojson_polygon(nl_poly)
        bbox = utils.bbox(polygon)
        grid = utils.create_rectangle_grid_morton(bbox=bbox,
                                                  hspacing=tilesize[0],
                                                  vspacing=tilesize[1])
        log.info(f"Nr. of tiles={len(grid)}")

    def test_export_tiles_int_cmd(self, cfg_db3dnl_int, data_dir,
                                  merge=False):
        """Test when the tile_index ID is an integer in the database, not a
        string."""
        dir = str(data_dir)
        tiles = ('1','2')
        path = Path(dir).resolve()
        if not Path(path.parent).exists():
            raise NotADirectoryError(f"Directory {path.parent} not exists")
        conn = db.Db(**cfg_db3dnl_int['database'])
        tile_index = db.Schema(cfg_db3dnl_int['tile_index'])
        tile_list = db3dnl.with_list(conn=conn, tile_index=tile_index,
                                     tile_list=tiles)
        if merge:
            filepath = (path / 'merged').with_suffix('.json')
            try:
                log.info(f"Exporting merged tiles {tiles}")
                cm = db3dnl.export(conn=conn,
                                   cfg=cfg_db3dnl_int,
                                   tile_list=tile_list)
                cityjson.save(cm, path=filepath, indent=None)
                log.info(f"Saved merged CityJSON tiles to {filepath}")
            except Exception as e:
                raise e
            finally:
                conn.close()
        else:
            try:
                for tile in tile_list:
                    log.info(f"Exporting tile {str(tile)}")
                    filepath = (path / str(tile)).with_suffix('.json')
                    cm = db3dnl.export(conn=conn,
                                       cfg=cfg_db3dnl_int,
                                       tile_list=(tile,))
                    cityjson.save(cm, path=filepath, indent=None)
                    log.info(f"Saved CityJSON tile {str(tile)} to {filepath}")
            except Exception as e:
                raise e
            finally:
                conn.close()

    def test_export_tiles_int_list_cmd(self, cfg_db3dnl_int, data_dir,
                                  merge=False):
        """Test when the tile_index ID is an integer in the database, not a
        string AND the tiles are a list, not a tuple."""
        dir = str(data_dir)
        tiles = ['1','2']
        path = Path(dir).resolve()
        if not Path(path.parent).exists():
            raise NotADirectoryError(f"Directory {path.parent} not exists")
        conn = db.Db(**cfg_db3dnl_int['database'])
        tile_index = db.Schema(cfg_db3dnl_int['tile_index'])
        tile_list = db3dnl.with_list(conn=conn, tile_index=tile_index,
                                     tile_list=tiles)
        if merge:
            filepath = (path / 'merged').with_suffix('.json')
            try:
                log.info(f"Exporting merged tiles {tiles}")
                cm = db3dnl.export(conn=conn,
                                   cfg=cfg_db3dnl_int,
                                   tile_list=tile_list)
                cityjson.save(cm, path=filepath, indent=None)
                log.info(f"Saved merged CityJSON tiles to {filepath}")
            except Exception as e:
                raise e
            finally:
                conn.close()
        else:
            try:
                for tile in tile_list:
                    log.info(f"Exporting tile {str(tile)}")
                    filepath = (path / str(tile)).with_suffix('.json')
                    cm = db3dnl.export(conn=conn,
                                       cfg=cfg_db3dnl_int,
                                       tile_list=(tile,))
                    cityjson.save(cm, path=filepath, indent=None)
                    log.info(f"Saved CityJSON tile {str(tile)} to {filepath}")
            except Exception as e:
                raise e
            finally:
                conn.close()