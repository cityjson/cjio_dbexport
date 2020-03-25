# -*- coding: utf-8 -*-
"""Configuration management.

Copyright (c) 2019, 3D geoinformation group, Delft University of Technology

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
from typing import TextIO, Mapping

import yaml

from cjio_dbexport import utils

log = logging.getLogger(__name__)


def verify_cotypes(cfg: Mapping) -> bool:
    """Verify that the configuration only contains the allowed CityObject types.

    .. note:: CityObjectGroup is not supported
    :raises: ValueError if invalid
    """
    # TODO: is it possible to extract the cityobject types from the cityjson schema?
    first_level = [
        'building',
        'road', 'railway', 'transportsquare',
        'tinrelief',
        'waterbody',
        'landuse',
        'plantcover',
        'solitaryvegetationobject',
        'cityfurniture',
        'genericcityobject',
        'bridge',
        'tunnel'
    ]
    second_level = [
        'buildingpart', 'buildinginstallation',
        'bridgepart', 'bridgeinstallation', 'bridgeconstructionelement',
        'tunnelpart', 'tunnelinstallation'
    ]
    if 'cityobject_type' not in cfg:
        raise ValueError(
            "The configuration file must have a member 'cityobject_type'")
    else:
        for cotype in cfg['cityobject_type']:
            _cotype = cotype.lower()
            if _cotype == 'cityobjectgroup':
                log.error("CityObjectGroup type is not supported")
            elif _cotype in second_level:
                f_lvl = _cotype.replace('installation', '').replace('part',
                                                                    '').replace(
                    'constructionelement', '')
                if f_lvl not in cfg['cityobject_type']:
                    raise ValueError(f"Cannot declare 2nd-level CityObject "
                                     f"{_cotype} by itself. It must have a "
                                     f"matching 1st-level CityObject that will "
                                     f"be used as parent.")
            elif _cotype not in first_level:
                raise ValueError(f"{_cotype} is not a valid CityObject type")
    return True


def add_lod_keys(cfg: Mapping) -> Mapping:
    """Add the lod-keys to the geometry fields of cityobject_type.

    If a CityObject mapping doesn't specify the LoD for the geometry, then add
    the global LoD to the geometry. The database export functions require that
    the geometry mapping declares the LoD.

    For instance convert this:

    .. code-block::

        lod: 1
        cityobject_type:
          Building:
            - schema: public
              table: building
              field:
                pk: ogc_fid
                geometry: wkb_geometry
                cityobject_id: identificatie

    To this:

    .. code-block::

        lod: 1
        cityobject_type:
          Building:
            - schema: public
              table: building
              field:
                pk: ogc_fid
                geometry:
                  lod1: wkb_geometry
                cityobject_id: identificatie

    """
    cfg_updated = cfg
    for cotype, relations in cfg['cityobject_type'].items():
        for i, relation in enumerate(relations):
            if isinstance(relation['field']['geometry'], str):
                lod_key = f"lod{cfg['geometries']['lod']}"
                lod_name_type = {
                    lod_key: {
                        'name': relation['field']['geometry'],
                        'type': 'MultiSurface'
                    }
                }
                cfg_updated['cityobject_type'][cotype][i]['field']['geometry'] = lod_name_type
            elif isinstance(relation['field']['geometry'], dict):
                lod_name_type = {}
                for lod_key in relation['field']['geometry']:
                    if lod_key[:3] != 'lod':
                        raise ValueError(
                            f"Incorrect 'geometry' field mapping in {relation}."
                            f" LoD key {lod_key} must begin with 'lod'.")
                    if not isinstance(relation['field']['geometry'][lod_key], dict):
                        raise ValueError(
                            f"Incorrect 'geometry' field mapping in {relation}."
                            f" {lod_key} must be a mapping.")
                    if not 'name' in relation['field']['geometry'][lod_key]:
                        raise ValueError(
                            f"Incorrect 'geometry' field mapping in {relation}."
                            f" Missing 'name' key.")
                    if 'type' in relation['field']['geometry'][lod_key]:
                        type = relation['field']['geometry'][lod_key]['type']
                    else:
                        type = cfg['geometries']['type']
                    lod_name_type[lod_key] = {
                        'name': relation['field']['geometry'][lod_key]['name'],
                        'type': type
                    }
                cfg_updated['cityobject_type'][cotype][i]['field']['geometry'] = lod_name_type
            else:
                raise ValueError(f"The 'geometry' field mapping must be a string"
                                 f" or a mapping in {relation}")
    return cfg_updated

# TODO refactor: move to the top
def parse_configuration(config: TextIO) -> Mapping:
    """Parse the configuration file.

    :return: The configuration as a dict
    """
    try:
        cfg_stream = yaml.load(config, Loader=yaml.FullLoader)
        log.debug(cfg_stream)
    except Exception as e:
        log.exception(e)
        raise
    try:
        verify_cotypes(cfg_stream)
    except ValueError as e:
        log.exception(e)
        raise
    try:
        lod_num = cfg_stream['geometries']['lod']
        cfg_stream['geometries']['lod'] = utils.lod_to_string(lod_num)
    except KeyError:
        log.exception("Did not find the 'lod' key in the configuration file")
        raise KeyError("Did not find the 'lod' key in the configuration file")
    except ValueError as e:
        log.exception(e)
        raise
    try:
        cfg_updated = add_lod_keys(cfg_stream)
        cfg_stream = cfg_updated
    except ValueError as e:
        log.exception(e)
        raise
    return cfg_stream
