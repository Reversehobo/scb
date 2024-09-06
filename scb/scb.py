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


def _get_partition_data(variables: dict, limit: int) -> tuple[dict, list]:
    """Get the number of batches and batch sizes for each variable.
    Args:
        variables (dict): A dictionary of variable names to lists of values.
        limit (int): The maximum number of rows to request.
    Returns:
        tuple: A dictionary of variable names to optimal batch sizes and a list of unique numbers of batches.
    """
    batch_size_sets = {}
    number_of_batches_lists = []
    for var, values in variables.items():
        size_to_batches = {}

        for size in range(1, min(len(values) + 1, limit + 1)):
            nbr_of_batches = math.ceil(len(values) / size)
            # This ensures we only keep the largest size for each unique number of batches
            # (since we want to minimize the number of requests)
            size_to_batches[nbr_of_batches] = size
        batch_size_sets[var] = size_to_batches
        number_of_batches_lists.append(list(size_to_batches.keys()))

    return batch_size_sets, number_of_batches_lists


def _find_optimal_combination(variables: dict, limit: int) -> dict:
    """Optimized function to find the optimal batch sizes for each variable.
    Args:
        variables (dict): A dictionary of variable names to lists of values.
        limit (int): The maximum number of rows to request.
    Returns:
        dict: A dictionary of variable names to optimal batch sizes.
    """
    total_rows = math.prod([len(values) for values in variables.values()])
    lower_request_bound = math.ceil(total_rows / limit)
    if lower_request_bound == 1:
        return {var: len(values) for var, values in variables.items()}

    batch_size_sets, number_of_batches_lists = _get_partition_data(variables, limit)

    best_combination = None
    min_request_count = float("inf")

    for combo in iter_product(*number_of_batches_lists):
        request_count = math.prod(combo)
        if request_count >= lower_request_bound and request_count < min_request_count:
            batch_sizes_product = math.prod(
                [batch_size_sets[var][nbr] for var, nbr in zip(variables.keys(), combo)]
            )
            if batch_sizes_product <= limit:
                min_request_count = request_count
                best_combination = combo
                if min_request_count == lower_request_bound:
                    break
    return {
        var: batch_size_sets[var][nbr]
        for var, nbr in zip(variables.keys(), best_combination)
    }


def _split_into_batches(values, batch_size):
    """Split values into batches of up to batch_size, with the last batch potentially smaller."""
    nbr_of_lists = math.ceil(len(values) / batch_size)
    batches = [
        values[i * batch_size : (i + 1) * batch_size] for i in range(nbr_of_lists)
    ]
    return batches


def _generate_all_combinations(variables, optimal_batch_sizes):
    """Generate all combinations of batches, one batch from each variable's batched lists of values."""
    # Split each variable's values into batches according to the optimal batch size
    all_batches = {
        var: _split_into_batches(values, optimal_batch_sizes[var])
        for var, values in variables.items()
    }
    # Generate the Cartesian product of all batches to form the configurations
    configurations = list(iter_product(*all_batches.values()))

    # Convert tuple configurations to dictionary format
    configurations_dicts = []
    for configuration in configurations:
        config_dict = {
            var: batch for var, batch in zip(variables.keys(), configuration)
        }
        configurations_dicts.append(config_dict)

    return configurations_dicts


def _get_request_configs(
    variables: dict, limit: int, return_optimal_batch_sizes: bool = False
) -> dict | tuple:
    """Get the optimal batch sizes and all possible combinations of batches.
    Args:
        variables (dict): A dictionary of variable names to lists of values.
        limit (int): The maximum number of rows to request.
        return_optimal_batch_sizes (bool): Whether to return the optimal batch sizes.
    Returns:
        dict or tuple: If return_optimal_batch_sizes is True, return a tuple of the optimal batch sizes and the request configurations. Otherwise, return the request configurations.
    """

    optimal_batch_sizes = _find_optimal_combination(variables, limit)
    request_configs = _generate_all_combinations(variables, optimal_batch_sizes)
    if return_optimal_batch_sizes:
        return optimal_batch_sizes, request_configs
    return request_configs


def _construct_query(request_config: dict) -> dict:
    """
    Helper function for constructing a payload for a given table using the request_config and response_format.
    Meant only for pxweb based api's.

    Args:
    - request_config: The configuration for the request.

    Returns:
    - A dictionary representing the payload.
    """

    payload = {"selection": []}

    for code, values in request_config.items():
        payload["selection"].append(
            {
                "variableCode": code,
                "valueCodes": [str(value) for value in values],
            }
        )
    return payload


def _get_queries(variables: dict, limit: int) -> list[dict]:
    """Get all possible queries for a given set of variables and a limit on the number of rows.
    Args:
        variables (dict): A dictionary of variable names to lists of values.
        limit (int): The maximum number of rows to request.
    Returns:
        list: A list of dictionaries representing the payloads.
    """
    request_configs = _get_request_configs(variables, limit)
    return [_construct_query(config) for config in request_configs]


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

    queries = _get_queries(simple_variables, limit)

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
