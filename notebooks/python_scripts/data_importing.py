import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import requests
import time


def JSON_import(path):
    df = pd.DataFrame()

    for i in os.listdir(path):
        if i.endswith(".json"):
            with open(str(path) + "/" + i, "r") as f:
                data = json.load(f)
            data = data["data"]["results"]
            try:
                if data[0] in data:
                    data_bool = True
            except:
                data_bool = False
            if data_bool:
                # get each property in json
                for index, item in enumerate(data):
                    temp_df = pd.json_normalize(data[index])
                    df = pd.concat([df, temp_df])
            else:
                continue

    # encoding for tags
    df_exploded = df.explode("tags").dropna(subset=["tags"])
    tag_dummies = pd.get_dummies(df_exploded["tags"])
    df_with_dummies = pd.concat([df_exploded[["property_id"]], tag_dummies], axis=1)
    df_encoded = df_with_dummies.groupby("property_id").max().reset_index()
    df_tags = pd.merge(
        df[["property_id"]], df_encoded, on="property_id", how="left"
    ).fillna(0)
    df_tags.iloc[:, 1:] = df_tags.iloc[:, 1:].astype(int)

    df = (
        df_tags.set_index("property_id")
        .join(df.set_index("property_id"), how="left")
        .reset_index()
    )
    df = df.drop(columns="tags")

    # droping unneeded columns
    df = df.drop(
        [
            "description.sold_date",
            "list_date",
            "last_update_date",
            "branding",
            "source",
            "matterport",
            "photos",
            "virtual_tours",
            "primary_photo.href",
            "source.plan_id",
            "source.agents",
            "source.spec_id",
            "source.type",
            "lead_attributes.show_contact_an_agent",
            "products.brand_name",
            "other_listings.rdc",
            "primary_photo",
            "products",
            "other_listings",
            "open_houses",
        ],
        axis=1,
    )

    # dtypes
    df = df.convert_dtypes()

    # dupes
    df = df.drop_duplicates(subset="property_id", keep="first")

    return df


def query_google_places(
    df, api_key, radius=150, place_type="bus_stop", batch_size=10, delay=0.1
):
    """
    Queries the Google Places API for each row in a DataFrame based on latitude and longitude.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    api_key (str): Google Places API key.
    radius (int): Search radius in meters (default is 150).
    place_type (str): Type of place to search for (default is 'bus_stop').

    Returns:
    pd.DataFrame: DataFrame containing the 'id' from the input DataFrame and the API response results.
    """  # noqa
    results = []

    for index, row in df.iterrows():
        latitude = row["location.address.coordinate.lat"]
        longitude = row["location.address.coordinate.lon"]
        place_id = row["listing_id"]
        # Construct the API URL
        url = (
            f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            f"?location={latitude},{longitude}"
            f"&radius={radius}"
            f"&type={place_type}"
            f"&key={api_key}"
        )

        # Make the request to Google Places API
        response = requests.get(url)
        data = response.json()
        if "results" in data:
            for place in data["results"]:
                # Store each result with the original ID
                results.append({"id": place_id, "place_name": place["name"]})
        if (index + 1) % batch_size == 0:
            time.sleep(delay)
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    return results_df
