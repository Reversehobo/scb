from itertools import product as iter_product
import math
import requests
import json

SESSION = requests.Session()
BASE_URL = "https://api.scb.se/ov0104/v2beta/api/v2"
DEFAULT_LANG = "sv"  # Default language currently "sv" and "en" is supported
DEFAULT_FORMAT = "json"  # Default format for data response
CONFIG = None
PAGE_SIZE = 5000
LIMIT = None

DEFAULT_FORMAT = "csv2"  # Default format for data response

import pandas as pd
from io import StringIO


def combine_csv_strings(csv_strings: list, file_name: str = "last_data.csv") -> str:
    combined_df = pd.concat([pd.read_csv(StringIO(csv_str)) for csv_str in csv_strings])

    # Save the combined data as 'last_data.csv'
    combined_df.to_csv(file_name, index=False)

    # Return the combined CSV string
    output = StringIO()
    combined_df.to_csv(output, index=False)
    return output.getvalue()


# "maxDataCells": 150000,
#     "maxCallsPerTimeWindow": 30,
#     "timeWindow": 10,
def get_config() -> dict:
    """Get the configuration and set it as a global variable."""
    global CONFIG, LIMIT
    if CONFIG is None:
        response = SESSION.get(BASE_URL + "/config")
        CONFIG = response.json()
        LIMIT = CONFIG["maxDataCells"]
    return CONFIG


def get_limit() -> int:
    """Get the limit for the number of rows to request."""
    if CONFIG is None:
        get_config()
    return LIMIT


def get_partition_data(variables: dict, limit: int) -> tuple[dict, list]:
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


def find_optimal_combination(variables: dict, limit: int) -> dict:
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

    batch_size_sets, number_of_batches_lists = get_partition_data(variables, limit)

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


def split_into_batches(values, batch_size):
    """Split values into batches of up to batch_size, with the last batch potentially smaller."""
    nbr_of_lists = math.ceil(len(values) / batch_size)
    batches = [
        values[i * batch_size : (i + 1) * batch_size] for i in range(nbr_of_lists)
    ]
    return batches


def generate_all_combinations(variables, optimal_batch_sizes):
    """Generate all combinations of batches, one batch from each variable's batched lists of values."""
    # Split each variable's values into batches according to the optimal batch size
    all_batches = {
        var: split_into_batches(values, optimal_batch_sizes[var])
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


def get_request_configs(
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

    optimal_batch_sizes = find_optimal_combination(variables, limit)
    request_configs = generate_all_combinations(variables, optimal_batch_sizes)
    if return_optimal_batch_sizes:
        return optimal_batch_sizes, request_configs
    return request_configs


def construct_query(request_config: dict) -> dict:
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


def get_variables(table_id: str, lang: str = DEFAULT_LANG) -> dict:
    """
    Fetch the variables for a specific table.
    Args:
        table_id (str): The table ID to fetch variables for.
        lang (str): The language for the response (default is 'en').
    Returns:
        dict: A dictionary of variables for the specified table.
    """
    url = f"{BASE_URL}/tables/{table_id}/metadata"
    params = {"lang": lang}
    response = SESSION.get(url, params=params)
    metadata = response.json()
    variables = {}
    for variable in metadata["variables"]:
        code = variable["id"]
        values = [value["code"] for value in variable["values"]]
        variables[code] = values

    return variables


def get_all_data(
    table_id: str,
    lang: str = DEFAULT_LANG,
    format: str = DEFAULT_FORMAT,
) -> dict:
    """
    Fetch all data for a specific table.

    Args:
        table_id: The ID of the table to fetch data from.
        lang: The language for the response (default is 'sv').
    """

    variables = get_variables(table_id, lang)
    limit = get_limit()

    url = f"{BASE_URL}/tables/{table_id}/data"
    params = {"lang": lang, "outputFormat": format}

    request_configs = get_request_configs(variables, limit)

    print(f"Number of requests: {len(request_configs)}")

    queries = [construct_query(config) for config in request_configs]

    data = []

    num_done = 0
    for query in queries:
        num_done += 1
        # response = requests.post(url, params=params, json=query)
        response = SESSION.post(url, params=params, json=query)

        data.append(response.text)
        print(f"Done with request {num_done}/{len(queries)}")

    # C:\Users\Admin\PYPI\scb\examples

    file_name = f"C:/Users/Admin/PYPI/scb/examples/{table_id}.csv"
    combined_data = combine_csv_strings(data, file_name)


table_id = "TAB638"
# table_id = "TAB2713"
table_id = "TAB1267"
get_all_data(table_id)
