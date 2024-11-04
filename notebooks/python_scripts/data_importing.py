import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from  dateutil.parser import parse
import requests
import urllib.parse
import requests
import time


def JSON_import(path):
    df = pd.DataFrame()

    for i in os.listdir(path):
        if i.endswith('.json'):
            with open(str(path)+'/'+i,'r') as f:
                data = json.load(f)
            data=data['data']['results']
            try:
                if data[0] in data:
                    data_bool = True
            except:
                data_bool = False
            if data_bool:
                # get each property in json
                for index, item in enumerate(data):
                    temp_df = pd.json_normalize(data[index])
                    df = pd.concat([df,temp_df])
            else:
                continue
        
    # encoding for tags
    df_exploded = df.explode('tags').dropna(subset=['tags'])
    tag_dummies = pd.get_dummies(df_exploded['tags'])
    df_with_dummies = pd.concat([df_exploded[['property_id']], tag_dummies], axis=1)
    df_encoded = df_with_dummies.groupby('property_id').max().reset_index()
    df_tags = pd.merge(df[['property_id']], df_encoded, on='property_id', how='left').fillna(0)
    df_tags.iloc[:, 1:] = df_tags.iloc[:, 1:].astype(int)

    df = df_tags.set_index('property_id').join(df.set_index('property_id'), how='left').reset_index()
    df = df.drop(columns='tags')

    # dtypes
    df = df.convert_dtypes()

    # dupes
    df = df.drop_duplicates(subset='property_id', keep='first')

    # correcting column names
    df.rename(columns={'location.address.state':'state','location.address.coordinate.lon':'coords_lon', 'location.address.coordinate.lat':'coords_lat', 'location.address.city':'city','location.address.line':'address', 'location.street_view_url':'street_view_url','location.address.postal_code':'zip_code','location.county.name':'county','description.year_built':'year_built','description.baths_3qtr':'baths_3qtr','description.sold_price':'sold_price','description.baths_full':'baths_full','description.name':'name','description.baths_half':'baths_half','description.lot_sqft':'lot_sqft','description.sqft':'sqft', 'description.baths':'baths', 'description.sub_type':'sub_type','description.baths_1qtr':'baths_1qtr','description.garage':'garage','description.stories':'stories','description.beds':'beds','description.type':'type'},inplace=True)
    
    # droping unneeded columns
    df=df.drop(['list_price','sub_type','price_reduced_amount','flags.is_subdivision', 'flags.is_contingent', 'flags.is_price_reduced', 'flags.is_pending', 'flags.is_foreclosure', 'flags.is_plan','listing_id','status','flags.is_new_listing','flags.is_coming_soon','location.address.coordinate','description.sold_date','list_date','location.address.state_code','last_update_date','branding', 'source','matterport', 'photos', 'virtual_tours','primary_photo.href','source.plan_id','source.agents','source.spec_id','source.type','lead_attributes.show_contact_an_agent','location.county.fips_code','products.brand_name','other_listings.rdc','primary_photo','products','other_listings', 'open_houses','community.advertisers','community.description.name','community.advertisers','location.county','flags.is_new_construction','flags.is_for_rent','baths_1qtr','name','community'], axis=1)
    
    return df


def missing_data(df):
    # inputing missing, but findable data
    df.at[23055,'coords_lon']=-74.76494
    df.at[23055,'coords_lat']=40.23233
    df.at[23055,'county']='Jones'

    df.at[25085,'county']='Carson City'

    df.at[22975,'address'] = '3 Hulse Street'

    # missing lat lon values from addresses


    # droping rows with missing lat, long, and addresses
    drop_rows = df[(df['address'].isna() & (df['coords_lat'].isna() & (df['coords_lon'].isna())))].index
    df.drop(drop_rows, inplace=True)

    # list_price
    # mean = df['list_price'].mean()
    # mean = round(mean,0)
    # df['list_price'].fillna(value=mean, inplace=True)

    # year_built
    mode = df['year_built'].mode()
    df['year_built'].fillna(value=mode, inplace=True)

    # baths_3qtr
    mode = df['baths_3qtr'].mode()
    df['baths_3qtr'].fillna(value=mode, inplace=True)

    # # sold_price
    # mean = df['sold_price'].mean()
    # df['sold_price'].fillna(value=mean, inplace=True)

    # baths_full
    mode = df['baths_full'].mode()
    df['baths_full'].fillna(value=mode, inplace=True)

    # baths_half
    mode = df['baths_half'].mode()
    df['baths_half'].fillna(value=mode, inplace=True)

    # lot_sqft
    mean = df['lot_sqft'].mean()
    df['lot_sqft'].fillna(value=mean, inplace=True)

    # sqft
    mean = df['sqft'].mean()
    df['sqft'].fillna(value=mean, inplace=True)

    # baths
    mode = df['baths'].mode()
    df['baths'].fillna(value=mode, inplace=True)

    # baths_1qtr                   
    mode = df['baths_1qtr'].mode()
    df['baths_1qtr'].fillna(value=mode, inplace=True)

    # garage
    mode = df['garage'].mode()
    df['garage'].fillna(value=mode, inplace=True)

    # stories
    mode = df['stories'].mode()
    df['stories'].fillna(value=mode, inplace=True)


    # beds
    mode = df['beds'].mode()
    df['beds'].fillna(value=mode, inplace=True)

    # type
    mode = df['type'].mode()
    df['beds'].fillna(value=mode, inplace=True)


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