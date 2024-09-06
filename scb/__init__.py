"""SCB Python Wrapper."""

from .scb import (
    get_config,
    get_tables,
    get_folder,
    get_data,
    get_metadata,
    get_variables,
    get_all_data,
)

__all__ = [
    "get_config",
    "get_tables",
    "get_folder",
    "get_data",
    "get_metadata",
    "get_variables",
    "get_all_data",
]
# Automatically load the configuration upon importing
get_config()
