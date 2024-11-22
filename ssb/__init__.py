"""SSB Python Wrapper."""

from scb import (
    get_config,
    get_tables,
    get_folder,
    get_data,
    get_metadata,
    get_variables,
    get_codelist,
    get_all_data,
    set_base_url,
    set_database,
    set_default_lang,
)

set_database("ssb")


__all__ = [
    "get_config",
    "get_tables",
    "get_folder",
    "get_data",
    "get_metadata",
    "get_variables",
    "get_codelist",
    "get_all_data",
    "set_base_url",
    "set_database",
    "set_default_lang",
]
# Automatically load the configuration upon importing
get_config()
