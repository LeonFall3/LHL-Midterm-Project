import time
import pandas as pd
import requests
import os


def query_by_lat_long(
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
        latitude = row["coords_lat"]
        longitude = row["coords_lon"]
        property_id = row["property_id"]
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
                results.append({"id": property_id, "place_name": place["name"]})
        if (index + 1) % batch_size == 0:
            time.sleep(delay)
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    return results_df


def get_lat_long(df, api_key, batch_size=10, delay=0.1):
    results = []

    for index, row in df.iterrows():
        address = row["address"]
        city = row["city"]
        state = row["state"]
        property_id = row["property_id"]

        url = "https://places.googleapis.com/v1/places:searchText"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": "places.location",
        }
        data = {"textQuery": f"{address} {city} {state}"}

        response = requests.post(url, headers=headers, json=data)
        data = response.json()

        # Checking we got data and then appending to list
        if "places" in data:
            for place in data["places"]:
                location = place.get("location", {})
                # Add the property ID, latitude, and longitude to results
                results.append(
                    {
                        "property_id": property_id,
                        "latitude": location.get("latitude"),
                        "longitude": location.get("longitude"),
                    }
                )
        else:
            print(f"No 'places' data found for {property_id}")

        # Pausing between batches to avoid hitting API limits
        if (index + 1) % batch_size == 0:
            time.sleep(delay)

    # Converting results to DataFrame
    results_df = pd.DataFrame(results, columns=["property_id", "latitude", "longitude"])
    return results_df


# def encode_tags(df):
#     """Use this function to manually encode tags from each sale.
#     You could also provide another argument to filter out low
#     counts of tags to keep cardinality to a minimum.

#     Args:
#         pandas.DataFrame

#     Returns:
#         pandas.DataFrame: modified with encoded tags
#     """
#     tags = df["tags"].tolist()
#     # create a unique list of tags and then create a new column for each tag

#     return df
