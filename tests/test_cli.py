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
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'Export tool from PostGIS to CityJSON' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert 'Export tool from PostGIS to CityJSON' in help_result.output

@pytest.mark.db3dnl
class TestDb3DNLIntegration:
    def test_export_tiles(self, data_dir, cfg_db3dnl_path, capsys):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path),
            'export_tiles',
            '--threads', '4',
            'gb1', 'ic3', 'kh7', 'ec4',
            str(data_dir)
        ])
        # log.info(f"\n{export_result.output}")
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export_tiles_merge(self, data_dir, cfg_db3dnl_path, capsys):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path),
            'export_tiles',
            '--merge',
            '--threads', '4',
            'gb1', 'ic3', 'kh7', 'ec4',
            str(data_dir)
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export(self, data_dir, cfg_db3dnl_path):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        outfile = str(data_dir / 'test.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path),
            'export',
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export_bbox(self, data_dir, cfg_db3dnl_path):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        outfile = str(data_dir / 'test_bbox.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path),
            'export_bbox',
            '92837.734', '465644.179', '193701.818', '466898.821',
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_export_extent(self, data_dir, cfg_db3dnl_path, nl_poly_path):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        outfile = str(data_dir / 'test_poly.json')
        export_result = runner.invoke(cli.main, [
            str(cfg_db3dnl_path),
            'export_extent',
            str(nl_poly_path),
            outfile
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()

    def test_index(self, data_dir, nl_poly_path, cfg_cjdb_path):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        export_result = runner.invoke(cli.main, [
            str(cfg_cjdb_path),
            'index',
            '--drop',
            str(nl_poly_path),
            '1000', '1000',
        ])
        if export_result.exit_code != 0:
            log.error(export_result.stderr_bytes)
            log.exception(export_result.exception)
            pytest.fail()
