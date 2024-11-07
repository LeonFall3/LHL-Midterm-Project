import time
import pandas as pd
import requests


def bus_query_by_lat_long(df, radius=150, batch_size=10, delay=0.1):
    """
    Queries the Overpass API, which is a free open-source api, for each row in a DataFrame based on latitude and longitude.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    radius (int): Search radius in meters (default is 150).
    Batch_size and delay are to time release the api calls.

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
            f"https://overpass-api.de/api/interpreter?data="
            f"[out:json];node(around:{radius},{latitude},{longitude})"
            f'["highway"="bus_stop"];out;'
        )

        # Make the request to Google Places API
        response = requests.get(url)
        data = response.json()
        if "results" in data:
            for place in data["results"]:
                # Store each result with the original ID
                place_name = place.get("tags", {}).get("name", "Unknown")
                results.append({"id": property_id, "place_name": place_name})
        if (index + 1) % batch_size == 0:
            time.sleep(delay)
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    counted_df = results_df["property_id"].value_counts().reset_index()
    counted_df.columns = ["property_id", "busstops"]
    return results_df


def get_lat_long(df, api_key, batch_size=1, delay=1):
    """
    This is an api call to geocode.maps to get a longitude and latitude for sales that are missing this information.

    Parameters:
    IMPORTANT: batch_size MUST BE 1 and delay MUST BE 1
    API has a limit of 1 call/second up to 5,000 per day

    df = dataframe with missing lat/longs
    api_key = api key from geocode.maps.co
    """  # noqa
    results = []

    for index, row in df.iterrows():
        address = row["address"]
        address = address.replace(" ", "+")
        city = row["city"]
        city = city.replace(" ", "+")
        state = row["state"]
        state = state.replace(" ", "+")
        property_id = index
        url = (
            f"https://geocode.maps.co/q?"
            f"street={address}&"
            f"city={city}&"
            f"state={state}&"
            f"api_key={api_key}"
        )

        response = requests.get(url)
        data = response.json()

        # Checking we got data and then appending to list
        if data:
            """
            If there are multiple businesses or if the address is an apartment building it will return one for each business or person listed on their api. Grabbing the first one since we only need the lat/long.
            """  # noqa
            first_one = data[0]
            for place in data["places"]:
                # Add the property ID, latitude, and longitude to results
                results.append(
                    {
                        "property_id": property_id,
                        "coords_lat": first_one.get("lat"),
                        "coords_lon": first_one.get("long"),
                    }
                )
        else:
            print(f"No 'places' data found for {property_id}")

        # Pausing between batches to avoid hitting API limits
        if (index + 1) % batch_size == 0:
            time.sleep(delay)

    # Converting results to DataFrame
    results_df = pd.DataFrame(
        results, columns=["property_id", "coords_lat", "coords_lon"]
    )
    results_df.set_index("property_id", inplace=True)
    return results_df
