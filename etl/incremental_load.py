import pandas as pd
import os

def incremental_merge(new_data, path, key):

    if os.path.exists(path):

        existing = pd.read_csv(path)

        merged = pd.concat([existing, new_data])

        merged = merged.drop_duplicates(subset=[key], keep="last")

    else:

        merged = new_data

    merged.to_csv(path, index=False)