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

    # dtypes
    df = df.convert_dtypes()
    df.set_index("property_id", inplace=True)

    # dupes
    df = df.drop_duplicates(subset="property_id", keep="first")

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
            "description.name": "name",
            "description.baths_half": "baths_half",
            "description.sqft": "sqft",
            "description.baths": "baths",
            "description.sub_type": "sub_type",
            "description.baths_1qtr": "baths_1qtr",
            "description.garage": "garage",
            "description.beds": "beds",
            "description.type": "type",
        },
        inplace=True,
    )

    # droping unneeded columns
    df = df.drop(
        [
            "list_price",
            "sub_type",
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
            "baths_1qtr",
            "name",
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

    return df


def missing_data(df):
    # setting the data types
    # inputing missing, but findable data
    df.at[23055, "coords_lon"] = -74.76494
    df.at[23055, "coords_lat"] = 40.23233

    df.at[22975, "address"] = "3 Hulse Street"

    df.at[26960, "city"] = "Columbus"

    df["type"] = df["type"].astype(object)

    # dropping rows with missing lat, long, and addresses
    drop_rows = df[
        (df["address"].isna()) & (df["coords_lat"].isna()) & (df["coords_lon"].isna())
    ].index
    df.drop(drop_rows, inplace=True)

    # Fill missing values for numeric columns with mean
    for column in df.select_dtypes(include="number").columns:
        df[column] = df[column].astype(float)
        df[column].fillna(df[column].mean(), inplace=True)

    # Fill missing values for categorical columns with mode
    for column in df.select_dtypes(include="object").columns:
        mode_value = df[column].mode()
        df[column].fillna(mode_value, inplace=True)

    mode_value = df["type"].mode()
    df["type"].fillna(mode_value[0], inplace=True)

    return df
