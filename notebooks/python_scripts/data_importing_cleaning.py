import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import requests
import urllib.parse
import requests
import time
from dotenv import load_dotenv

load_dotenv()
import sys

sys.path.append(r"notebooks\python_scripts")
from API_calls import *


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

    # removing all dupes of property_id before making it the index
    df = df.drop_duplicates(subset="property_id", keep="first")
    df.set_index("property_id", inplace=True)

    return df


def cleaning_data(df):
    # key for getting missing address coords
    api_key = "GEOCODING_API_KEY"

    # droping unneeded columns
    df = df.drop(
        [
            "list_price",
            "description.sub_type",
            "price_reduced_amount",
            "flags.is_subdivision",
            "flags.is_contingent",
            "flags.is_price_reduced",
            "flags.is_pending",
            "flags.is_foreclosure",
            "flags.is_plan",
            "listing_id",
            "status",
            "flags.is_new_listing",
            "flags.is_coming_soon",
            "location.address.coordinate",
            "description.sold_date",
            "list_date",
            "location.address.state_code",
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
            "location.county.fips_code",
            "products.brand_name",
            "other_listings.rdc",
            "primary_photo",
            "products",
            "other_listings",
            "open_houses",
            "community.advertisers",
            "community.description.name",
            "community.advertisers",
            "location.county",
            "flags.is_new_construction",
            "flags.is_for_rent",
            "description.baths_1qtr",
            "description.name",
            "community",
            "description.year_built",
            "location.address.postal_code",
            "description.stories",
            "location.street_view_url",
            "description.baths_3qtr",
            "description.lot_sqft",
            "location.county.name",
        ],
        axis=1,
    )
    print("Columns Dropped")

    # correcting column names
    df.rename(
        columns={
            "location.address.state": "state",
            "location.address.coordinate.lon": "coords_lon",
            "location.address.coordinate.lat": "coords_lat",
            "location.address.city": "city",
            "location.address.line": "address",
            "description.sold_price": "sold_price",
            "description.baths_full": "baths_full",
            "description.baths_half": "baths_half",
            "description.sqft": "sqft",
            "description.baths": "baths",
            "description.garage": "garage",
            "description.beds": "beds",
            "description.type": "type",
        },
        inplace=True,
    )
    print("Columns Renamed")

    # cleaning column 'type'
    condo_lst = ["condos", "apartment", "condo_townhome_rowhome_coop"]
    single_family_lst = ["triplex_duplex", "other"]

    df["type"] = df["type"].apply(lambda x: "condo" if x in condo_lst else x)
    df["type"] = df["type"].apply(
        lambda x: "single_family" if x in single_family_lst else x
    )
    print(r"Column 'type' grouped")

    # dropping missing addresses
    df = df[~df["address"].isna()]

    # Creating the missing lat/long data frame
    missing_lat_lon = df[df["coords_lat"].isna() | df["coords_lon"].isna()]

    # finding missing coords via API
    results_df, drops = get_lat_long(missing_lat_lon, api_key)
    print("requested missing coords from API")

    # Update missing 'coords_lon' and 'coords_lat' with newly founded coords
    df["coords_lon"].fillna(results_df["coords_lon"])
    df["coords_lat"].fillna(results_df["coords_lat"])

    # Drop the extra columns
    # df = results_df.drop(columns=["coords_lon", "coords_lat"])
    print("Merged missing coords with main data")

    df = df[~df["coords_lon"].isna()]
    df = df[~df["coords_lat"].isna()]
    print("dropped rows without coords")

    df = df.convert_dtypes()
    # Fill missing values for numeric columns with mean
    for column in df.select_dtypes(include="number").columns:
        if column in ["coords_lon", "coords_lat"]:
            df[column] = df[column].astype(float)
        else:
            df[column].fillna(df[column].mean().round(0), inplace=True)
            df[column]
            df[column] = df[column].astype(int)

    # Fill missing values for categorical columns with mode
    for column in df.select_dtypes(include="object").columns:
        mode_value = df[column].mode()
        df[column].fillna(mode_value, inplace=True)
        df[column]=df[column].astype(str)
    print("Missing num and cat data filled with mean/mode and dtypes set")

    # dropping land sales
    df.drop(df[df["type"] == "land"].index, inplace=True)
    print("dropped all land sales")

    return df
