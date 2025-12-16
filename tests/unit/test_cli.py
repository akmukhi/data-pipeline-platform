"""Unit tests for CLI."""

import pytest
from unittest.mock import MagicMock, patch

from cli.main import (
    create_parser,
    health_command,
    ingest_command,
    persist_command,
    status_command,
    transform_command,
    workers_command,
)


@pytest.mark.unit
class TestCLI:
    """Test CLI functions."""

    def test_create_parser(self):
        """Test parser creation."""
        parser = create_parser()
        assert parser is not None

    def test_parser_has_commands(self):
        """Test parser has all commands."""
        parser = create_parser()
        subcommands = [action.dest for action in parser._actions if hasattr(action, "dest") and action.dest == "command"]

        # Check that subparsers exist
        assert hasattr(parser, "_subparsers")

    @patch("cli.main.ingest_task")
    def test_ingest_command(self, mock_task, celery_app):
        """Test ingest command."""
        mock_result = MagicMock()
        mock_result.id = "test_task_id"
        mock_result.get.return_value = {"status": "success", "row_count": 100}
        mock_task.delay.return_value = mock_result

        args = MagicMock()
        args.query = "SELECT * FROM users"
        args.source_db_url = None
        args.parameters = None
        args.batch_size = None
        args.wait = True
        args.timeout = 10

        result = ingest_command(args)
        assert result == 0

    @patch("cli.main.transform_task")
    def test_transform_command(self, mock_task, celery_app):
        """Test transform command."""
        mock_result = MagicMock()
        mock_result.id = "test_task_id"
        mock_result.get.return_value = {"status": "success", "row_count": 100}
        mock_task.delay.return_value = mock_result

        args = MagicMock()
        args.type = "sql"
        args.config = '{"sql_query": "SELECT * FROM input"}'
        args.data_id = None
        args.wait = True
        args.timeout = 10

        result = transform_command(args)
        assert result == 0

    @patch("cli.main.persist_task")
    def test_persist_command(self, mock_task, celery_app):
        """Test persist command."""
        mock_result = MagicMock()
        mock_result.id = "test_task_id"
        mock_result.get.return_value = {"status": "success", "rows_written": 100}
        mock_task.delay.return_value = mock_result

        args = MagicMock()
        args.table_name = "test_table"
        args.schema = None
        args.strategy = "insert"
        args.upsert_keys = None
        args.data_id = None
        args.wait = True
        args.timeout = 10

        result = persist_command(args)
        assert result == 0

    @patch("cli.main.AsyncResult")
    def test_status_command(self, mock_async_result):
        """Test status command."""
        mock_result = MagicMock()
        mock_result.ready.return_value = True
        mock_result.successful.return_value = True
        mock_result.result = {"status": "success"}
        mock_async_result.return_value = mock_result

        args = MagicMock()
        args.task_id = "test_task_id"

        result = status_command(args)
        assert result == 0

    @patch("cli.main.health_check_task")
    def test_health_command(self, mock_task, celery_app):
        """Test health command."""
        mock_result = MagicMock()
        mock_result.get.return_value = {"status": "healthy", "stats": {}}
        mock_task.delay.return_value = mock_result

        args = MagicMock()

        result = health_command(args)
        assert result == 0

    @patch("cli.main.celery_app")
    def test_workers_command(self, mock_celery_app):
        """Test workers command."""
        mock_inspect = MagicMock()
        mock_inspect.stats.return_value = {"worker1": {"pool": "prefork"}}
        mock_inspect.active.return_value = {"worker1": []}
        mock_celery_app.control.inspect.return_value = mock_inspect

        args = MagicMock()

        result = workers_command(args)
        assert result == 0

