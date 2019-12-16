#!/usr/bin/env python

"""Tests for `cjio_dbexport` package."""

import pytest

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
