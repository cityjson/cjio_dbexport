# -*- coding: utf-8 -*-
"""Testing the configuration handling"""
import yaml
import pytest

from cjio_dbexport import configure

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