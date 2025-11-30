"""Configuration settings for DDB CLI."""

import os


def get_darango_api() -> str:
    """Get the Darango API URL from environment or default."""
    return os.environ.get("DARANGO_API", "http://localhost:8080")


def get_default_db() -> str:
    """Get the default database name from environment or default."""
    return os.environ.get("ARANGO_DB", "_system")
