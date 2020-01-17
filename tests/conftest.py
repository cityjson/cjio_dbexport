# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
from pathlib import Path
import pytest

from cjio_dbexport import configure, db

#------------------------------------ add option for running the full test set
def pytest_addoption(parser):
    parser.addoption("--rundb3dnl", action="store_true",
                     default=False, help="run tests against the 3DNL database")
    parser.addoption("--runcjdb", action="store_true",
                     default=False, help="run tests against the cjdb_test database")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--rundb3dnl"):
        return
    if config.getoption("--runcjdb"):
        return
    skip_db3dnl = pytest.mark.skip(reason="need --rundb3dnl option to run")
    skip_cjdb = pytest.mark.skip(reason="need --runcjdb option to run")
    for item in items:
        if "db3dnl" in item.keywords:
            item.add_marker(skip_db3dnl)
        if "cjdb" in item.keywords:
            item.add_marker(skip_cjdb)

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
    yield root_dir / 'cjio_dbexport'


@pytest.fixture(scope='function')
def cfg_open(data_dir):
    config = data_dir / 'test_config.yml'
    with open(config, 'r') as fo:
        yield fo


# -------------------------------------------------------------------- testing DB

@pytest.fixture('session')
def cfg_parsed(data_dir):
    config = data_dir / 'db3dnl_config.yml'
    with open(config, 'r') as fo:
        c = configure.parse_configuration(fo)
        yield c


@pytest.fixture('session')
def db3dnl_db(cfg_parsed):
    # TODO: needs database setup
    conn = db.Db(**cfg_parsed['database'])
    yield conn
    conn.close()

@pytest.fixture(scope='function')
def nl_poly(data_dir):
    with open(data_dir / 'nl_single.geojson', 'r') as fo:
        yield fo

@pytest.fixture(scope='function')
def nl_multi(data_dir):
    with open(data_dir / 'nl_multi.geojson', 'r') as fo:
        yield fo


@pytest.fixture('session')
def cfg_cjdb(data_dir):
    config = data_dir / 'test_config.yml'
    with open(config, 'r') as fo:
        c = configure.parse_configuration(fo)
        yield c

@pytest.fixture('session')
def cjdb_db(cfg_cjdb):
    # TODO: needs database setup
    conn = db.Db(**cfg_cjdb['database'])
    yield conn
    conn.close()

@pytest.fixture('session')
def tin_schema(cfg_parsed):
    yield db.Schema(cfg_parsed['features'])

