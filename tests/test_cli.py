#!/usr/bin/env python
"""Tests for `cjio_dbexport` package."""

import pytest
import logging

log = logging.getLogger(__name__)

from click.testing import CliRunner

from cjio_dbexport import cli


def test_command_line_interface():
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main, ['--help'])
        assert result.exit_code == 0
        assert 'Export tool from PostGIS to CityJSON' in result.output

@pytest.mark.db3dnl
class TestDb3DNLIntegration:
    def test_export_tiles(self, data_output_dir, cfg_db3dnl_path_param, capfd):
        """Test the CLI."""
        runner = CliRunner()
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path_param),
            'export_tiles',
            '--jobs', '1',
            'gb1', 'ic3', 'kh7', 'ec4',
            str(data_output_dir)
        ], catch_exceptions=False)
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()
        if any(True for res in ['ERROR', 'CRITICAL', 'FATAL']
               if res in export_result.output):
                pytest.fail()

    def test_export_tiles_merge(self, data_output_dir, cfg_db3dnl_path_param, capfd):
        """Test the CLI."""
        runner = CliRunner()
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path_param),
            'export_tiles',
            '--merge',
            '--jobs', '4',
            'gb1', 'ic3', 'kh7', 'ec4',
            str(data_output_dir)
        ])
        print(export_result.output)
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()
        if any(True for res in ['ERROR', 'CRITICAL', 'FATAL']
               if res in export_result.output):
                pytest.fail()

    @pytest.mark.skip
    def test_export(self, data_output_dir, cfg_db3dnl_path_param):
        """Test the CLI."""
        runner = CliRunner()
        outfile = str(data_output_dir / 'test.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path_param),
            'export',
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export_bbox(self, data_output_dir, cfg_db3dnl_path_param):
        """Test the CLI."""
        runner = CliRunner()
        outfile = str(data_output_dir / 'test_bbox.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path_param),
            'export_bbox',
            '92837.734', '465644.179', '193701.818', '466898.821',
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export_extent(self, data_output_dir, cfg_db3dnl_path_param, db3dnl_poly_geojson):
        """Test the CLI."""
        runner = CliRunner()
        outfile = str(data_output_dir / 'test_poly.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path_param),
            'export_extent',
            str(db3dnl_poly_geojson),
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_index(self, db3dnl_poly_geojson, cfg_cjdb_path, caplog):
        """Test the CLI."""
        runner = CliRunner()
        export_result = runner.invoke(cli.main, [
            str(cfg_cjdb_path),
            'index',
            '--drop',
            str(db3dnl_poly_geojson),
            '100', '100',
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()


@pytest.mark.db3dnl
class TestLoD2Integration:
    def test_export_one(self, data_output_dir, cfg_lod2_path_param, capfd):
        """Test the CLI."""
        runner = CliRunner()
        export_result = runner.invoke(cli.main, [
            str(cfg_lod2_path_param),
            'export_tiles',
            '--jobs', '1',
            'ec4',
            str(data_output_dir)
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()
        if any(True for res in ['ERROR', 'CRITICAL', 'FATAL']
               if res in export_result.output):
                pytest.fail()
