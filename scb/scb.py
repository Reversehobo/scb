import json
from . import SESSION

BASE_URL = "https://api.scb.se/ov0104/v2beta/api/v2"
DEFAULT_LANG = "sv"  # Default language currently "sv" and "en" is supported
DEFAULT_FORMAT = "json"  # Default format for data response
CONFIG = None
PAGE_SIZE = 5000


def get_config() -> dict:
    """Get the configuration and set it as a global variable."""
    global CONFIG
    if CONFIG is None:
        response = SESSION.get(BASE_URL + "/config")
        CONFIG = response.json()
    return CONFIG


def get_folder(folder_id: str = "", lang: str = DEFAULT_LANG) -> dict:
    """Get the folder information for a specific folder ID."""
    response = SESSION.get(f"{BASE_URL}/navigation/{folder_id}", params={"lang": lang})
    return response.json()


def get_tables(
    lang: str = DEFAULT_LANG,
    query: str = None,
    past_days: int = None,
    include_discontinued: bool = True,
) -> dict:
    """
    Fetch available tables with various filter options.

    Args:
        lang: The language for the response (default is 'en').
        query: Search criteria to filter tables by name or other attributes.
        past_days: Filters tables that were updated in the last N days.
        include_discontinued: Whether to include discontinued tables (default is False).

    Returns:
        A dictionary of available tables matching the search criteria.
    """
    url = f"{BASE_URL}/tables"
    params = {
        "lang": lang,
        "query": query,
        "pastDays": past_days,
        "includeDiscontinued": str(
            include_discontinued
        ).lower(),  # API expects a string 'true' or 'false'
        "pageNumber": 1,
        "pageSize": PAGE_SIZE,
    }

    # Remove None values from the params dictionary
    params = {k: v for k, v in params.items() if v is not None}

    response = SESSION.get(url, params=params)
    return response.json()
