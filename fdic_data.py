import pandas as pd
import requests
from functools import lru_cache


def get_data(file_path, scraping_function):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Failed to read {file_path}. Calling {scraping_function.__name__} instead...")
        df = scraping_function()
    
    return df.copy()

@lru_cache(maxsize=None) # note generally a great idea to not set a maxsize
def get_deposit_data():
    limit = 10000

    url = "https://banks.data.fdic.gov/api/sod"

    years = [year for year in range(1994, 2023)]  # there is no data for 2023

    params = {
        "limit": limit,
        "format": "json",
    }

    deposits_data = []

    counts_by_year = {}

    for year in years:
        print("Year processing: ", year)

        year_filter = f"YEAR:{year}"

        first_request = requests.get(url, params={**params, "filters": year_filter})

        first_response = first_request.json()

        total_records = first_response["totals"]["count"]

        counts_by_year[year] = total_records

        for offset in range(0, total_records, limit):
            request = requests.get(
                url, params={**params, "filters": year_filter, "offset": offset}
            )
            response = request.json()
            yearly_data_offset_chunk = [datum["data"] for datum in response["data"]]

            deposits_data = [*deposits_data, *yearly_data_offset_chunk]

    return pd.DataFrame(deposits_data)


@lru_cache(maxsize=None)
def get_failure_data():
    failures_request = requests.get("https://banks.data.fdic.gov/api/Failures?limit=10000")
    failures_json = failures_request.json()
    failure_data = [datum["data"] for datum in failures_json["data"]]
    return pd.DataFrame(failure_data)