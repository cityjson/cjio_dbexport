# -*- coding: utf-8 -*-
"""Testing the configuration handling"""
import yaml
import pytest

from cjio_dbexport import configure

@pytest.fixture(scope='function')
def cfg_open(cfg_cjdb_path):
    with open(cfg_cjdb_path, 'r') as fo:
        yield fo

@pytest.fixture(scope='function')
def cfg_multi_lod():
    cfg = """
    lod: 1

    database:
      dbname: cjdb_test
      host: localhost
      port: 5432
      user: cjdb_tester
      password: cjdb_test1234
    
    tile_index:
      schema: tile_index
      table: tile_index_1
      srid: 7415
      field:
        pk: id
        geometry: geom
    
    cityobject_type:
      Building:
        - schema: public
          table: building
          field:
            pk: ogc_fid
            geometry: 
              lod12: geometry_lod12
              lod0: geometry_lod0
            cityobject_id: identificatie
            exclude: ["xml", "_clipped"]
      Road:
        - schema: public
          table: wegdeel_vlak
          field:
            pk: ogc_fid
            geometry: wkb_geometry
            cityobject_id: identificatie
            exclude: ["xml"]
      TINRelief:
        - schema: public
          table: tintable
          field:
            pk: fid
            geometry:
              lod2: geometry_lod2
            cityobject_id: coid
    """
    yield yaml.load(cfg, Loader=yaml.FullLoader)

class TestConfigure:
    def test_parse_configuration(self, cfg_open):
        cfg = configure.parse_configuration(cfg_open)
        assert 'onbegroeidterreindeel_vlak' in cfg['cityobject_type']['LandUse'][0]['table']

    def test_verify_cotypes(self, cfg_open):
        cfg = yaml.load(cfg_open, Loader=yaml.FullLoader)
        assert configure.verify_cotypes(cfg)

    def test_verify_cotypes_invalid_2nd(self, cfg_open):
        """Invalid 2nd-level type"""
        cfg = yaml.load(cfg_open, Loader=yaml.FullLoader)
        cfg['cityobject_type']['BridgePart'] = cfg['cityobject_type']['Bridge']
        del cfg['cityobject_type']['Bridge']
        with pytest.raises(ValueError):
            configure.verify_cotypes(cfg)

    def test_verify_cotypes_invalid_1st(self, cfg_open):
        """Invalid 2nd-level type"""
        cfg = yaml.load(cfg_open, Loader=yaml.FullLoader)
        cfg['cityobject_type']['invalid_type'] = cfg['cityobject_type']['Bridge']
        del cfg['cityobject_type']['Bridge']
        with pytest.raises(ValueError):
            configure.verify_cotypes(cfg)

    def test_add_lod_param(self):
        """Adding the global LoD parameter to each geometry"""
        cfg = """
        geometries:
          lod: 1
          type: MultiSurface

        database:
          dbname: cjdb_test
          host: localhost
          port: 5432
          user: cjdb_tester
          password: cjdb_test1234

        tile_index:
          schema: tile_index
          table: tile_index_1
          srid: 7415
          field:
            pk: id
            geometry: geom

        cityobject_type:
          Building:
            - schema: public
              table: building
              field:
                pk: ogc_fid
                geometry: 
                  lod12: 
                    name: geometry_lod12
                    type: Solid
                  lod0: 
                    name: geometry_lod0
                cityobject_id: identificatie
                exclude: ["xml", "_clipped"]
          Road:
            - schema: public
              table: wegdeel_vlak
              field:
                pk: ogc_fid
                geometry: wkb_geometry
                cityobject_id: identificatie
                exclude: ["xml"]
          TINRelief:
            - schema: public
              table: tintable
              field:
                pk: fid
                geometry:
                  lod2: 
                    name: geometry_lod2
                cityobject_id: coid
        """
        cfg = yaml.load(cfg, Loader=yaml.FullLoader)

        expect = """
        geometries:
          lod: 1
          type: MultiSurface

        database:
          dbname: cjdb_test
          host: localhost
          port: 5432
          user: cjdb_tester
          password: cjdb_test1234

        tile_index:
          schema: tile_index
          table: tile_index_1
          srid: 7415
          field:
            pk: id
            geometry: geom

        cityobject_type:
          Building:
            - schema: public
              table: building
              field:
                pk: ogc_fid
                geometry: 
                  lod12: 
                    name: geometry_lod12
                    type: Solid
                  lod0: 
                    name: geometry_lod0
                    type: MultiSurface
                cityobject_id: identificatie
                exclude: ["xml", "_clipped"]
          Road:
            - schema: public
              table: wegdeel_vlak
              field:
                pk: ogc_fid
                geometry: 
                  lod1: 
                    name: wkb_geometry
                    type: MultiSurface
                cityobject_id: identificatie
                exclude: ["xml"]
          TINRelief:
            - schema: public
              table: tintable
              field:
                pk: fid
                geometry:
                  lod2: 
                    name: geometry_lod2
                    type: MultiSurface
                cityobject_id: coid
        """
        expect = yaml.load(expect, Loader=yaml.FullLoader)
        result = configure.add_lod_keys(cfg)
        assert result == expect