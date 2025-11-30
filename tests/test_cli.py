"""Tests for the DDB CLI."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ddb.cli import app
from ddb.client import DarangoClient, DarangoError


@pytest.fixture
def runner():
    """Create a CLI runner."""
    return CliRunner()


@pytest.fixture
def mock_client():
    """Create a mock Darango client."""
    with patch("ddb.cli.get_client") as mock:
        client = MagicMock(spec=DarangoClient)
        mock.return_value = client
        yield client


class TestCreateCommand:
    """Tests for the create command."""

    def test_create_collection_success(self, runner, mock_client):
        """Test successful collection creation."""
        mock_client.create_collection.return_value = {
            "id": "12345",
            "name": "test_collection",
            "status": 3,
            "type": 2,
        }

        result = runner.invoke(app, ["create", "test_collection", "--db", "_system"])

        assert result.exit_code == 0
        assert "test_collection" in result.stdout
        mock_client.create_collection.assert_called_once_with(
            "_system", "test_collection", "document"
        )

    def test_create_edge_collection(self, runner, mock_client):
        """Test creating an edge collection."""
        mock_client.create_collection.return_value = {
            "id": "12345",
            "name": "edges",
            "type": 3,
        }

        result = runner.invoke(
            app, ["create", "edges", "--db", "_system", "--type", "edge"]
        )

        assert result.exit_code == 0
        mock_client.create_collection.assert_called_once_with(
            "_system", "edges", "edge"
        )

    def test_create_collection_error(self, runner, mock_client):
        """Test collection creation error handling."""
        mock_client.create_collection.side_effect = DarangoError(
            "Collection already exists", 409
        )

        result = runner.invoke(app, ["create", "existing", "--db", "_system"])

        assert result.exit_code == 1
        assert "Error" in result.stdout
        assert "Collection already exists" in result.stdout


class TestQueryCommand:
    """Tests for the query command."""

    def test_query_success(self, runner, mock_client):
        """Test successful query execution."""
        mock_client.query.return_value = {
            "result": [{"_key": "1", "value": 42}],
            "hasMore": False,
        }

        result = runner.invoke(
            app, ["query", "FOR doc IN test RETURN doc", "--db", "_system"]
        )

        assert result.exit_code == 0
        assert "42" in result.stdout
        mock_client.query.assert_called_once_with(
            "_system", "FOR doc IN test RETURN doc", None
        )

    def test_query_with_bind_vars(self, runner, mock_client):
        """Test query with bind variables."""
        mock_client.query.return_value = {"result": [], "hasMore": False}

        result = runner.invoke(
            app,
            [
                "query",
                "FOR doc IN @@col RETURN doc",
                "--db",
                "_system",
                "--bind",
                '{"@col": "test"}',
            ],
        )

        assert result.exit_code == 0
        mock_client.query.assert_called_once_with(
            "_system", "FOR doc IN @@col RETURN doc", {"@col": "test"}
        )

    def test_query_invalid_bind_json(self, runner, mock_client):
        """Test query with invalid bind variable JSON."""
        result = runner.invoke(
            app,
            [
                "query",
                "FOR doc IN test RETURN doc",
                "--db",
                "_system",
                "--bind",
                "invalid json",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid JSON" in result.stdout


class TestGetCommand:
    """Tests for the get command."""

    def test_get_document_success(self, runner, mock_client):
        """Test successful document retrieval."""
        mock_client.get_document.return_value = {
            "_key": "doc1",
            "_id": "test/doc1",
            "value": 100,
        }

        result = runner.invoke(
            app, ["get", "--db", "_system", "--col", "test", "--key", "doc1"]
        )

        assert result.exit_code == 0
        assert "doc1" in result.stdout
        assert "100" in result.stdout
        mock_client.get_document.assert_called_once_with("_system", "test", "doc1")

    def test_get_document_not_found(self, runner, mock_client):
        """Test document not found error."""
        mock_client.get_document.side_effect = DarangoError("Document not found", 404)

        result = runner.invoke(
            app, ["get", "--db", "_system", "--col", "test", "--key", "missing"]
        )

        assert result.exit_code == 1
        assert "Document not found" in result.stdout


class TestInsertCommand:
    """Tests for the insert command."""

    def test_insert_document_success(self, runner, mock_client):
        """Test successful document insertion."""
        mock_client.insert_document.return_value = {
            "_key": "k1",
            "_id": "test/k1",
            "_rev": "12345",
        }

        result = runner.invoke(
            app,
            [
                "insert",
                "--db",
                "_system",
                "--col",
                "test",
                "--doc",
                '{"_key": "k1", "value": 42}',
            ],
        )

        assert result.exit_code == 0
        assert "k1" in result.stdout
        mock_client.insert_document.assert_called_once_with(
            "_system", "test", {"_key": "k1", "value": 42}
        )

    def test_insert_invalid_json(self, runner, mock_client):
        """Test insert with invalid JSON document."""
        result = runner.invoke(
            app,
            ["insert", "--db", "_system", "--col", "test", "--doc", "not json"],
        )

        assert result.exit_code == 1
        assert "Invalid JSON" in result.stdout


class TestUpdateCommand:
    """Tests for the update command."""

    def test_update_document_success(self, runner, mock_client):
        """Test successful document update."""
        mock_client.update_document.return_value = {
            "_key": "k1",
            "_id": "test/k1",
            "_rev": "67890",
        }

        result = runner.invoke(
            app,
            [
                "update",
                "--db",
                "_system",
                "--col",
                "test",
                "--key",
                "k1",
                "--doc",
                '{"value": 100}',
            ],
        )

        assert result.exit_code == 0
        mock_client.update_document.assert_called_once_with(
            "_system", "test", "k1", {"value": 100}
        )

    def test_update_invalid_json(self, runner, mock_client):
        """Test update with invalid JSON document."""
        result = runner.invoke(
            app,
            [
                "update",
                "--db",
                "_system",
                "--col",
                "test",
                "--key",
                "k1",
                "--doc",
                "bad",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid JSON" in result.stdout


class TestDeleteCommand:
    """Tests for the delete command."""

    def test_delete_document_success(self, runner, mock_client):
        """Test successful document deletion."""
        mock_client.delete_document.return_value = {"_key": "k1", "_id": "test/k1"}

        result = runner.invoke(
            app, ["delete", "--db", "_system", "--col", "test", "--key", "k1"]
        )

        assert result.exit_code == 0
        mock_client.delete_document.assert_called_once_with("_system", "test", "k1")

    def test_delete_document_not_found(self, runner, mock_client):
        """Test delete document not found error."""
        mock_client.delete_document.side_effect = DarangoError(
            "Document not found", 404
        )

        result = runner.invoke(
            app, ["delete", "--db", "_system", "--col", "test", "--key", "missing"]
        )

        assert result.exit_code == 1
        assert "Document not found" in result.stdout


class TestDefaultDatabase:
    """Tests for default database handling."""

    def test_uses_default_database(self, runner, mock_client):
        """Test that commands use default database when not specified."""
        mock_client.create_collection.return_value = {"name": "test"}

        with patch("ddb.cli.get_default_db", return_value="mydb"):
            result = runner.invoke(app, ["create", "test"])

        assert result.exit_code == 0
        mock_client.create_collection.assert_called_once_with(
            "mydb", "test", "document"
        )


class TestClientConfig:
    """Tests for client configuration."""

    def test_client_uses_env_config(self):
        """Test that client reads configuration from environment."""
        with patch.dict("os.environ", {"DARANGO_API": "http://custom:9090"}):
            from ddb.config import get_darango_api

            assert get_darango_api() == "http://custom:9090"

    def test_client_default_config(self):
        """Test client default configuration."""
        with patch.dict("os.environ", {}, clear=True):
            from ddb.config import get_darango_api

            assert get_darango_api() == "http://localhost:8080"
