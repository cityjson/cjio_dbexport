# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
from pathlib import Path
import pytest

from cjio_dbexport import configure, db


#-------------------------------------------------------------------- directory
@pytest.fixture('session')
def t_dir():
    """tests directory"""
    yield Path(__file__).parent


@pytest.fixture('session')
def data_dir(t_dir):
    yield t_dir / 'data'


@pytest.fixture('session')
def root_dir(t_dir):
    yield t_dir.parent


@pytest.fixture('session')
def package_dir(root_dir):
    yield root_dir / 'tin'


# -------------------------------------------------------------------- testing DB
@pytest.fixture('session')
def cfg(data_dir):
    config = data_dir / 'balazs_config.yml'
    with open(config, 'r') as fo:
        c = configure.parse_configuration(fo)
        yield c


@pytest.fixture('session')
def db3dnl_db(cfg):
    # TODO: needs database setup
    conn = db.Db(**cfg['database'])
    yield conn
    conn.close()


@pytest.fixture('session')
def tin_schema(cfg):
    yield db.Schema(cfg['features'])

