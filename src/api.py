import pandas as pd
import requests
from pandas import json_normalize


def get_api_data(path: str) -> pd.DataFrame:

    headers = {
        "accept": "application/json",
    }

    response = requests.get(
        path,
        headers=headers,
    )

    return json_normalize(response.json())
