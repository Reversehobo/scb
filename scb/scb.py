import json
from request_builder import get_queries
import time
import csv
from io import StringIO
import requests
from itertools import product as iter_product
import math


SESSION = requests.Session()
BASE_URL = "https://api.scb.se/ov0104/v2beta/api/v2"
DEFAULT_LANG = "sv"  # Default language currently "sv" and "en" is supported
DEFAULT_FORMAT = "csv2"  # Default format for data response
CONFIG = None
PAGE_SIZE = 10000


def _combine_csv_strings(csv_strings: list[str]) -> str:
    output = []
    for i, csv_string in enumerate(csv_strings):
        reader = csv.reader(StringIO(csv_string))
        if i == 0:
            output.extend(reader)
        else:
            output.extend(row for j, row in enumerate(reader) if j > 0)

    result = StringIO()
    writer = csv.writer(result)
    writer.writerows(output)
    return result.getvalue()


def _simplify_string(string: str) -> str:
    """Simplify a string by stripping, removing spaces, and lowercasing."""
    return string.strip().replace(" ", "").lower()


def _compare_strings(string: str, *args: str) -> bool:
    """Returns True if the string is equal to any of the given arguments."""
    simple_string = _simplify_string(string)
    for arg in args:
        if simple_string == _simplify_string(arg):
            return True
    return False


def _save_csv(data: str, file_name: str) -> None:
    """Save the data as a CSV file."""
    with open(file_name, "w", encoding="utf-8", newline="") as file:
        file.write(data)


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
    response_dict = response.json()
    tables = response_dict["tables"]
    # remove the links and type element from each table, also remove description if empty string
    for table in tables:
        table.pop("links", None)
        table.pop("type", None)
        if table["description"] == "":
            table.pop("description", None)

    return tables


def get_metadata(table_id: str, lang: str = DEFAULT_LANG) -> dict:
    """
    Fetch metadata for a specific table.

    Args:
        table_id: The ID of the table to fetch metadata for.
        lang: The language for the response. Optional.
    """
    url = f"{BASE_URL}/tables/{table_id}/metadata"
    params = {"lang": lang}
    response = SESSION.get(url, params=params)
    return response.json()


def get_variables(table_id: str, lang: str = DEFAULT_LANG) -> dict:
    """
    Fetch the variables for a specific table.

    Args:
        table_id: The table ID to fetch variables for.
        lang: The language for the response (default is 'en').

    Returns:
        A dictionary of variables for the specified table.
    """
    metadata = get_metadata(table_id, lang)
    return metadata["variables"]


def get_data(
    table_id: str, lang: str = DEFAULT_LANG, format: str = DEFAULT_FORMAT, **kwargs
):
    """
    Fetch data for a specific table.

    Args:
        table_id: The ID of the table to fetch data from.
        lang: The language for the response (default is 'en').
        format: The format for the data response (default is 'json').
        kwargs: The query to filter the data.

    Returns:
        A dictionary of data for the specified table.
    """
    metadata = get_metadata(table_id, lang)
    variables = metadata["variables"]
    query = {"selection": []}

    for kwarg in kwargs:
        for variable in variables:
            # if variable["label"] == kwarg or variable["id"] == kwarg:
            if _compare_strings(kwarg, variable["label"], variable["id"]):
                input_values = kwargs[kwarg]

                query["selection"].append(
                    {
                        "variableCode": variable["id"],
                        "valueCodes": [
                            value["code"]
                            for value in variable["values"]
                            if _compare_strings(value["label"], *input_values)
                            or _compare_strings(value["code"], *input_values)
                        ],
                    }
                )

    url = f"{BASE_URL}/tables/{table_id}/data"
    params = {"lang": lang, "outputFormat": format}
    response = SESSION.post(url, params=params, json=query)

    if "json" in response.headers["Content-Type"]:
        return response.json()
    else:
        return response.text


def get_all_data(table_id: str, lang: str = DEFAULT_LANG, file_name: str = None):
    """
    Fetch all data for a specific table.

    Args:
        table_id: The ID of the table to fetch data from.
        lang: The language for the response (default is 'sv').
        file_name: The name of the file to save the data to (optional).
    """

    variables = get_variables(table_id, lang)
    limit = CONFIG["maxDataCells"]
    min_request_interval = CONFIG["timeWindow"] / CONFIG["maxCallsPerTimeWindow"] + 0.1
    last_request_time = time.time()

    simple_variables = {}
    for variable in variables:
        simple_variables[variable["id"]] = [
            value["code"] for value in variable["values"]
        ]

    queries = get_queries(simple_variables, limit)

    data = []
    url = f"{BASE_URL}/tables/{table_id}/data"
    params = {"lang": lang, "outputFormat": "csv2"}

    data = []

    for query in queries:
        current_time = time.time()
        if current_time - last_request_time < min_request_interval:
            time.sleep(min_request_interval - (current_time - last_request_time))
        last_request_time = time.time()

        response = SESSION.post(url, params=params, json=query)
        data.append(response.text)

    if file_name:
        _save_csv(_combine_csv_strings(data), file_name)

    return _combine_csv_strings(data)
