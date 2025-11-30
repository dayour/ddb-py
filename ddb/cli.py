"""Typer-based CLI for DDB."""

import json
from typing import Any, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.json import JSON

from ddb.client import DarangoClient, DarangoError
from ddb.config import get_default_db

app = typer.Typer(
    name="ddb",
    help="CLI for interacting with Darango API",
    add_completion=False,
)
console = Console()


def get_client() -> DarangoClient:
    """Get a configured Darango client."""
    return DarangoClient()


def print_json(data: dict[str, Any]) -> None:
    """Print JSON data with rich formatting."""
    console.print(JSON(json.dumps(data)))


def print_error(message: str) -> None:
    """Print an error message."""
    rprint(f"[red]Error:[/red] {message}")


@app.command()
def create(
    name: str = typer.Argument(..., help="Name of the collection to create"),
    db: str = typer.Option(None, "--db", help="Database name"),
    collection_type: str = typer.Option(
        "document", "--type", help="Collection type (document or edge)"
    ),
) -> None:
    """Create a new collection."""
    db = db or get_default_db()
    client = get_client()
    try:
        result = client.create_collection(db, name, collection_type)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


@app.command()
def query(
    aql: str = typer.Argument(..., help="AQL query to execute"),
    db: str = typer.Option(None, "--db", help="Database name"),
    bind: Optional[str] = typer.Option(
        None, "--bind", help="Bind variables as JSON string"
    ),
) -> None:
    """Execute an AQL query."""
    db = db or get_default_db()
    client = get_client()
    bind_vars = None
    if bind:
        try:
            bind_vars = json.loads(bind)
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON for bind variables: {e}")
            raise typer.Exit(1)
    try:
        result = client.query(db, aql, bind_vars)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


@app.command("get")
def get_doc(
    db: str = typer.Option(None, "--db", help="Database name"),
    col: str = typer.Option(..., "--col", help="Collection name"),
    key: str = typer.Option(..., "--key", help="Document key"),
) -> None:
    """Retrieve a document by key."""
    db = db or get_default_db()
    client = get_client()
    try:
        result = client.get_document(db, col, key)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


@app.command()
def insert(
    db: str = typer.Option(None, "--db", help="Database name"),
    col: str = typer.Option(..., "--col", help="Collection name"),
    doc: str = typer.Option(..., "--doc", help="Document as JSON string"),
) -> None:
    """Insert a new document."""
    db = db or get_default_db()
    client = get_client()
    try:
        document = json.loads(doc)
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON for document: {e}")
        raise typer.Exit(1)
    try:
        result = client.insert_document(db, col, document)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


@app.command()
def update(
    db: str = typer.Option(None, "--db", help="Database name"),
    col: str = typer.Option(..., "--col", help="Collection name"),
    key: str = typer.Option(..., "--key", help="Document key"),
    doc: str = typer.Option(..., "--doc", help="Updated document as JSON string"),
) -> None:
    """Update an existing document."""
    db = db or get_default_db()
    client = get_client()
    try:
        document = json.loads(doc)
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON for document: {e}")
        raise typer.Exit(1)
    try:
        result = client.update_document(db, col, key, document)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


@app.command()
def delete(
    db: str = typer.Option(None, "--db", help="Database name"),
    col: str = typer.Option(..., "--col", help="Collection name"),
    key: str = typer.Option(..., "--key", help="Document key"),
) -> None:
    """Delete a document."""
    db = db or get_default_db()
    client = get_client()
    try:
        result = client.delete_document(db, col, key)
        print_json(result)
    except DarangoError as e:
        print_error(e.message)
        raise typer.Exit(1)


def main() -> None:
    """Run the CLI application."""
    app()


if __name__ == "__main__":
    main()
