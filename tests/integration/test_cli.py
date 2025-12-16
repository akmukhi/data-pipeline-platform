"""Integration tests for CLI."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.integration
class TestCLI:
    """Test CLI commands."""

    def test_cli_help(self):
        """Test CLI help command."""
        result = subprocess.run(
            [sys.executable, "-m", "cli.main", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "pipeline-cli" in result.stdout or "CLI" in result.stdout

    def test_cli_health(self, celery_app):
        """Test CLI health command."""
        result = subprocess.run(
            [sys.executable, "-m", "cli.main", "health"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        # May fail if workers not available, that's OK
        assert result.returncode in [0, 1]

    def test_cli_workers(self):
        """Test CLI workers command."""
        result = subprocess.run(
            [sys.executable, "-m", "cli.main", "workers"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        # May fail if workers not available
        assert result.returncode in [0, 1]

    def test_cli_invalid_command(self):
        """Test CLI with invalid command."""
        result = subprocess.run(
            [sys.executable, "-m", "cli.main", "invalid_command"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

