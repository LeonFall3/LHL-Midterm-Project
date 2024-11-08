import time
import pandas as pd
import requests
import overpy

# %%
import overpy

api = overpy.Overpass()
radius = 1
latitude = 38.043246
longitude = -77.505373
result = api.query(
    f"""node(around:{radius},{latitude},{longitude})["public_transport"="platform"]["bus"="yes"];out;"""
)
print("Number of bus platforms found:", len(result.nodes))


# %%
def bus_query_by_lat_long(df, radius=750, batch_size=10, delay=0.1):
    """
    Queries the Overpass API, which is a free open-source api, for each row in a DataFrame based on latitude and longitude.

    There is no API key required.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    radius (int): Search radius in meters (default is 150).
    Batch_size and delay are to time release the api calls.

    Returns:
    pd.DataFrame: DataFrame containing the 'id' from the input DataFrame and the API response results.
    """  # noqa
    results = []
    api = overpy.Overpass()
    total_rows = len(df)
    counter = 0
    for index, row in df.iterrows():
        latitude = row["coords_lat"]
        longitude = row["coords_lon"]
        property_id = index
        # Construct the API
        result = api.query(
            f"""node(around:{radius},{latitude},{longitude})["public_transport"="platform"]["bus"="yes"];out;"""
        )

        # Make the request to Overpass API

        results.append({"property_id": property_id, "busstops": len(result.nodes)})
        counter += 1
        progress = counter / total_rows
        if progress in [0.2, 0.4, 0.6, 0.8, 1.0]:
            print(f"{int(progress * 100)}% done")
        if (int(index) + 1) % batch_size == 0:
            time.sleep(delay)
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
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
    drops = []

    for index, row in df.iterrows():
        data = None
        address = row["address"]
        address = address.replace(" ", "+")
        city = row["city"]
        city = city.replace(" ", "+")
        state = row["state"]
        state = state.replace(" ", "+")
        property_id = index
        if (int(index) + 1) % batch_size == 0:
            time.sleep(delay)
        url = (
            f"https://geocode.maps.co/search?"
            f"street={address}&"
            f"city={city}&"
            f"state={state}&"
            f"api_key={api_key}"
        )

        response = requests.get(url)
        if response.status_code == 200 and response.text.strip():
            try:
                data = response.json()
            except:
                continue

        # Checking we got data and then appending to list
        if data:
            """
            If there are multiple businesses or if the address is an apartment building it will return one for each business or person listed on their api. Grabbing the first one since we only need the lat/long.
            """  # noqa
            first_one = data[0]
            # Add the property ID, latitude, and longitude to results
            results.append(
                {
                    "property_id": property_id,
                    "coords_lat": first_one.get("lat"),
                    "coords_lon": first_one.get("long"),
                }
            )
        else:
            drops.append(property_id)

    # Converting results to DataFrame
    results_df = pd.DataFrame(
        results, columns=["property_id", "coords_lat", "coords_lon"]
    )
    results_df.set_index("property_id", inplace=True)
    return results_df
