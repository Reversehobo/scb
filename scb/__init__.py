"""SCB Python Wrapper."""

import requests

# Initialize the session globally

# BASE_URL = "https://api.scb.se/ov0104/v2beta/api/v2"
# DEFAULT_LANG = "sv"  # Default language currently "sv" and "en" is supported
# DEFAULT_FORMAT = "json"  # Default format for data response
SESSION = requests.Session()

from .scb import get_config, tables

__all__ = ["get_config", "tables"]

# Automatically load the configuration upon importing
get_config()


# import requests

# session = requests.Session()

# from .scb_old import SCB
