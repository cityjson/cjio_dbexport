# -*- coding: utf-8 -*-
"""Testing the 3DNL exporter"""

import logging
import pickle
import json
from concurrent.futures import as_completed, ThreadPoolExecutor

import pytest

import cjio_dbexport.utils
from cjio_dbexport import db3dnl, db, utils, cli

log = logging.getLogger(__name__)


@pytest.mark.db3dnl
class TestParsing:
    def test_parse_boundary(self):
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

    def test_build_query_all(self, db3dnl_db, cfg_db3dnl, caplog):
        features = db.Schema(cfg_db3dnl['cityobject_type']['LandUse'][0])
        tile_index = db.Schema(cfg_db3dnl['tile_index'])
        query = db3dnl.build_query(conn=db3dnl_db, features=features,
                                   tile_index=tile_index)
        query_str = db3dnl_db.print_query(query)
        assert '"xml"' not in query_str
        assert 'Exporting the whole database' in caplog.text

    def test_build_query_bbox(self, db3dnl_db, cfg_db3dnl, caplog):
        features = db.Schema(cfg_db3dnl['cityobject_type']['LandUse'][0])
        tile_index = db.Schema(cfg_db3dnl['tile_index'])
        query = db3dnl.build_query(conn=db3dnl_db, features=features,
                                   tile_index=tile_index,
                                   bbox=[192837.734, 465644.179, 193701.818,
                                         466898.821])
        query_str = db3dnl_db.print_query(query)
        assert '"xml"' not in query_str
        assert 'Exporting with BBOX' in caplog.text

    def test_build_query_extent(self, db3dnl_db, cfg_db3dnl, db3dnl_poly,
                                caplog):
        features = db.Schema(cfg_db3dnl['cityobject_type']['LandUse'][0])
        tile_index = db.Schema(cfg_db3dnl['tile_index'])
        query = db3dnl.build_query(conn=db3dnl_db, features=features,
                                   tile_index=tile_index,
                                   extent=db3dnl_poly)
        query_str = db3dnl_db.print_query(query)
        assert '"xml"' not in query_str
        assert 'Exporting with polygon' in caplog.text

    def test_build_query_tiles(self, db3dnl_db, cfg_db3dnl, caplog):
        features = db.Schema(cfg_db3dnl['cityobject_type']['LandUse'][0])
        tile_index = db.Schema(cfg_db3dnl['tile_index'])
        query = db3dnl.build_query(conn=db3dnl_db, features=features,
                                   tile_index=tile_index,
                                   tile_list=('ic2',))
        query_str = db3dnl_db.print_query(query)
        assert '"xml"' not in query_str and '"identificatie"' in query_str
        assert 'Exporting with a list of tiles' in caplog.text


# @pytest.mark.db3dnl
class TestIntegration:
    """Integration tests"""

    @pytest.mark.skip
    def test_export_all(self, data_dir, cfg_db3dnl, db3dnl_db, caplog):
        caplog.set_level(logging.DEBUG)
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'])
        dbexport = list(export_gen)

    def test_export_bbox(self, data_dir, cfg_db3dnl, db3dnl_db):
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'],
                                  bbox=[192837.734, 465644.179, 193701.818,
                                        466898.821])
        dbexport = list(export_gen)

    def test_export_extent(self, data_dir, cfg_db3dnl, db3dnl_db, db3dnl_poly):
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'], extent=db3dnl_poly)
        dbexport = list(export_gen)

    def test_export_tile_list(self, data_dir, cfg_db3dnl, db3dnl_db,
                              db3dnl_4tiles_pickle):
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'],
                                  tile_list=['gb2', 'ic1', 'ic2', 'ec4'])
        dbexport = list(export_gen)
        with open(db3dnl_4tiles_pickle, 'wb') as fo:
            pickle.dump(dbexport, fo)

    def test_export_tile_list_without_intersect(self, data_dir, cfg_db3dnl, db3dnl_db,
                                                db3dnl_4tiles_pickle):
        """Export a list of tiles when the object table has a tile ID column so they
        can be filtered directly on the tile id

        Needs:

            CREATE OR REPLACE VIEW building_tiles AS
            SELECT b.*, i.id AS _tile_id
            FROM building b
                     JOIN tile_index.tile_index_sub i
                          ON st_within(st_centroid(b.wkb_geometry), i.geom);
        """

        co = {"Building": cfg_db3dnl['cityobject_type']["Building"]}
        co["Building"][0]["table"] = "building_tiles"
        co["Building"][0]["field"]["tile"] = "_tile_id"
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'],
                                  tile_list=['gb2', 'ic1', 'ic2', 'ec4'])
        dbexport = list(export_gen)

    def test_export_tile_list_one(self, data_dir, cfg_db3dnl, db3dnl_db,
                                  db3dnl_4tiles_pickle):
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'], tile_list=['gb2', ])
        dbexport = list(export_gen)

    def test_query_no_export(self, data_dir, cfg_db3dnl, db3dnl_db,
                             db3dnl_4tiles_pickle):
        """Test that the query works when the 'exclude' key is not present"""
        for name, cotype in cfg_db3dnl['cityobject_type'].items():
            for tablename in cotype:
                tablename['field'].pop('exclude')
        export_gen = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                  tile_index=cfg_db3dnl['tile_index'],
                                  cityobject_type=cfg_db3dnl[
                                      'cityobject_type'], tile_list=['gb2', ])
        dbexport = list(export_gen)

    def test_convert(self, data_dir, db3dnl_4tiles_pickle, cfg_db3dnl, caplog):
        caplog.set_level(logging.DEBUG)
        # dbexport = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
        #                           tile_index=cfg_db3dnl['tile_index'],
        #                           cityobject_type=cfg_db3dnl[
        #                               'cityobject_type'], tile_list=['gb2', 'ic1', 'ic2', 'ec4'])
        # with open(data_dir / 'db3dnl_4tiles.pickle', 'wb') as fo:
        #     pickle.dump(list(dbexport), fo)
        with open(db3dnl_4tiles_pickle, 'rb') as fo:
            dbexport = pickle.load(fo)
        cm = db3dnl.convert(dbexport, cfg=cfg_db3dnl)
        assert len(cm.get_metadata()["geographicalExtent"]) > 0

        log.info(json.dumps(cm.get_metadata(), indent=2))
        with open(data_dir / '4tiles_cm.pickle', 'wb') as fo:
            pickle.dump(cm, fo)

    def test_export_convert_save(self, data_dir, cfg_db3dnl, db3dnl_db, caplog,
                                 data_output_dir):
        """Does the .convert method work and can we save a CityJSON file?"""
        caplog.set_level(logging.DEBUG)
        dbexport = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                tile_index=cfg_db3dnl['tile_index'],
                                cityobject_type=cfg_db3dnl[
                                    'cityobject_type'], tile_list=['gb2', ],
                                threads=1)
        cm = db3dnl.convert(dbexport, cfg=cfg_db3dnl)
        cm.get_info()
        outfile = data_output_dir / "gb2.city.json"
        cli.save(cm, outfile, indent=True)

    def test_export_lod_column(self, data_dir, db3dnl_db, cfg_db3dnl):
        c = [{
            'schema': 'public',
            'table': 'building',
            'field': {
                'pk': 'ogc_fid',
                'geometry': {'lod1': {'name': 'wkb_geometry', 'type': 'MultiSurface'}},
                'lod': '_lod',
                'cityobject_id': 'identificatie',
                'exclude': ['xml', '_clipped']
            }
        }]
        cfg_db3dnl['cityobject_type']['Building'] = c
        dbexport = db3dnl.query(conn_cfg=cfg_db3dnl['database'],
                                tile_index=cfg_db3dnl['tile_index'],
                                cityobject_type=cfg_db3dnl[
                                    'cityobject_type'], tile_list=['gb2', ],
                                threads=1)
        cm = db3dnl.convert(dbexport, cfg=cfg_db3dnl)
        assert 'LoD = [1.0, 1.2, 1.3]' in cm.get_info(long=True)

    def test_index(self, data_dir, nl_poly):
        tilesize = (10000, 10000)
        polygon = cjio_dbexport.utils.read_geojson_polygon(nl_poly)
        bbox = utils.bbox(polygon)
        grid = utils.create_rectangle_grid_morton(bbox=bbox,
                                                  hspacing=tilesize[0],
                                                  vspacing=tilesize[1])
        log.info(f"Nr. of tiles={len(grid)}")

    # def test_save(self, data_dir):
    #     with open(data_dir / '4tiles_cm.pickle', 'rb') as fo:
    #         cm = pickle.load(fo)
    #     db3dnl.save(cm, (data_dir / '4tiles_test').with_suffix('.json'))

    # def test_export_tiles_int_cmd(self, cfg_db3dnl_int, data_dir,
    #                               merge=False):
    #     """Test when the tile_index ID is an integer in the database, not a
    #     string."""
    #     dir = str(data_dir)
    #     tiles = ('1', '2')

    def test_export_features(self, data_dir, cfg_db3dnl, db3dnl_db, caplog,
                             data_output_dir):
        """Does the .convert method work and can we save a CityJSON file?"""
        caplog.set_level(logging.DEBUG)
        outfile = data_output_dir / "gb2.city.json"
        db3dnl.export(tile="gb2", filepath=outfile, cfg=cfg_db3dnl, zip=False,
                      features=True)

    # @pytest.mark.skip
    def test_export_tiles_multiprocess_features(self, db3dnl_db, cfg_db3dnl,
                                                data_output_dir):
        """Test when the tile_index ID is an integer in the database, not a
        string AND the tiles are a list, not a tuple."""
        tile_index = db.Schema(cfg_db3dnl['tile_index'])
        tile_list = db3dnl.with_list(conn=db3dnl_db, tile_index=tile_index,
                                     tile_list=('all',))

        db3dnl.export_tiles_multiprocess(cfg=cfg_db3dnl, jobs=4,
                                         path=data_output_dir, tile_list=tile_list,
                                         zip=False, features=True)


class TestLoD2:
    def test_export_lod2(self, cfg_lod2, caplog):
        caplog.set_level(logging.DEBUG)
        dbexport = db3dnl.query(conn_cfg=cfg_lod2['database'],
                                tile_index=cfg_lod2['tile_index'],
                                cityobject_type=cfg_lod2['cityobject_type'],
                                tile_list=['2', ])
        cm = db3dnl.convert(dbexport, cfg=cfg_lod2)
        log.info(cm.get_info(long=True))
        cm_sub = cm.get_subset_random(5)
        res = cm_sub.validate(longerr=True)
        log.info(res)
        assert res[0]
