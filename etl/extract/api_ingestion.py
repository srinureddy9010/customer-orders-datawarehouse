import requests
import pandas as pd


def fetch_api_data(url):
    """
    Fetch JSON data from API and convert to DataFrame
    """

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")

    data = response.json()

    df = pd.DataFrame(data)

    return df