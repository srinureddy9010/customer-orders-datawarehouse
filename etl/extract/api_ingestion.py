import requests
import pandas as pd

def fetch_api_data(url):

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()

    return pd.DataFrame(data)