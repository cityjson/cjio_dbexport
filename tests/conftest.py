# -*- coding: utf-8 -*-

"""pytest configuration"""


from pathlib import Path

import pgutils
import pytest
import yaml

from cjio_dbexport import configure, utils


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
@pytest.fixture(scope='session')
def t_dir():
    """tests directory"""
    return Path(__file__).parent


@pytest.fixture(scope='session')
def data_dir(t_dir):
    return t_dir / 'data'

@pytest.fixture(scope='session')
def data_output_dir(t_dir):
    outdir = t_dir / 'data' / 'output'
    outdir.mkdir(exist_ok=True)
    return outdir

@pytest.fixture(scope='session')
def root_dir(t_dir):
    return t_dir.parent


@pytest.fixture(scope='session')
def package_dir(root_dir):
    return root_dir / 'cjio_dbexport'

# ------------------------------------------------------------------- testing DB

@pytest.fixture(scope='session')
def cfg_db3dnl_path(data_dir):
    return data_dir / 'db3dnl_config.yml'


# params = [{"postgis-10-2.5": 5557}, {"postgis-17-3.5": 5558}],
# ids = ["postgis-10-2.5", "postgis-17-3.5"]
@pytest.fixture(scope='session',
                params=[{"postgis-17-3.5": 5558}],
                ids=["postgis-17-3.5"]
                )
def cfg_db3dnl(request, cfg_db3dnl_path):
    with open(cfg_db3dnl_path, 'r') as fo:
        c = configure.parse_configuration(fo)
        postgis_docker, port = list(request.param.items())[0]
        c["database"]["port"] = port
        return c

@pytest.fixture(scope='function',
                params=[{"postgis-17-3.5": 5558}],
                ids=["postgis-17-3.5"])
def cfg_db3dnl_path_param(request, cfg_db3dnl_path, data_output_dir):
    with open(cfg_db3dnl_path, 'r') as fo:
        c = configure.parse_configuration(fo)
        postgis_docker,port = list(request.param.items())[0]
        c["database"]["port"] = port
        outpath = data_output_dir / Path(postgis_docker).with_suffix('.yml')
    with outpath.open('w') as fo:
        yaml.dump(c, fo)
    return outpath


@pytest.fixture(scope='function',
                params=[{"postgis-17-3.5": 5558}],
                ids=["postgis-17-3.5"])
def cfg_db3dnl_int(request, data_dir):
    config = data_dir / 'db3dnl_config_int.yml'
    with open(config, 'r') as fo:
        c = configure.parse_configuration(fo)
        postgis_docker, port = list(request.param.items())[0]
        c["database"]["port"] = port
        yield c

@pytest.fixture(scope='function')
def db3dnl_poly(data_dir):
    # db3dnl_poly.geojson
    return [ [ [ 80033.149634172412334, 448713.694506597646978 ], [ 79728.150836692744633, 447643.321368272707332 ], [ 80119.470048553455854, 447355.586653669248335 ], [ 80482.015788953824085, 447372.850736545457039 ], [ 80683.430089176254114, 447608.793202520289924 ], [ 80850.316223646266735, 448224.545491771714296 ], [ 81080.503995329039753, 448276.337740400340408 ], [ 81253.144824091126793, 448604.355315048305783 ], [ 80994.183580948010786, 448857.561863899347372 ], [ 80464.751706077615381, 448903.599418235942721 ], [ 80033.149634172412334, 448713.694506597646978 ] ] ]

@pytest.fixture(scope='function')
def db3dnl_poly_geojson(data_dir):
    yield data_dir / 'db3dnl_poly.geojson'

@pytest.fixture(scope='function')
def db3dnl_4tiles_pickle(data_dir):
    yield data_dir / 'db3dnl_4tiles.pickle'

@pytest.fixture(scope='session')
def db3dnl_db(cfg_db3dnl):
    # TODO: needs database setup
    conn = pgutils.PostgresConnection(**cfg_db3dnl['database'])
    assert utils.create_functions(conn)
    yield conn

@pytest.fixture(scope='function')
def nl_poly_path(data_dir):
    yield data_dir / 'nl_single.geojson'

@pytest.fixture(scope='function')
def nl_poly(nl_poly_path):
    with open(nl_poly_path, 'r') as fo:
        yield fo

@pytest.fixture(scope='function')
def nl_multi(data_dir):
    with open(data_dir / 'nl_multi.geojson', 'r') as fo:
        yield fo


@pytest.fixture(scope='function')
def cfg_lod2_path(data_dir):
    return data_dir / 'db3dbag_config_lod2.yml'

@pytest.fixture(scope='function',
                params=[{"postgis-17-3.5": 5558}],
                ids=["postgis-17-3.5"])
def cfg_lod2_path_param(request, cfg_lod2_path, data_output_dir):
    with open(cfg_lod2_path, 'r') as fo:
        c = configure.parse_configuration(fo)
        postgis_docker,port = list(request.param.items())[0]
        c["database"]["port"] = port
        outpath = data_output_dir / Path(postgis_docker).with_suffix('.yml')
    with outpath.open('w') as fo:
        yaml.dump(c, fo)
    return outpath

@pytest.fixture(scope='function')
def cfg_lod2(cfg_lod2_path):
    """YAML config with the LoD2.2 table"""
    with open(cfg_lod2_path, 'r') as fo:
        return configure.parse_configuration(fo)


# ------------------------------------------------------------------------- cjdb

@pytest.fixture(scope='function')
def cfg_cjdb_path(data_dir):
    yield data_dir / 'test_config.yml'

@pytest.fixture(scope='function')
def cfg_cjdb(cfg_cjdb_path):
    with open(cfg_cjdb_path, 'r') as fo:
        c = configure.parse_configuration(fo)
        yield c

@pytest.fixture(scope='function')
def cjdb_db(cfg_cjdb):
    conn = pgutils.PostgresConnection(**cfg_cjdb['database'])
    yield conn
