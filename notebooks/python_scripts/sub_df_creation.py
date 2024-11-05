import pandas as pd
import numpy as np

def create_tags_df(df):
    """
    return a df with the count of each tag per property
    """
    tags_df = df.drop(['county','street_view_url','address','city','coords_lat','coords_lon','state','zip_code','type','beds','stories','garage','baths','sqft','lot_sqft','baths_half','baths_full','baths_3qtr','year_built','permalink'],axis=1)

    tag_count = pd.DataFrame(tags_df.apply(lambda x: (x == 1.0).sum()), columns=['count_1.0_values'])
    tag_count = tag_count.drop(['property_id','sold_price'])

    return tags_df

def create_tags_totals_df(df):
    """ 
    return a df with the total tag count of each property
    """
    tags_df = create_tags_df(df)
    tags_df=tags_df.T
    tags_df.columns = tags_df.iloc[0]
    tags_df = tags_df[1:]
    tags_df = tags_df.apply(pd.value_counts)
    tags_df=tags_df.T
    tags_df = tags_df[[1.0]]
    tags_df= tags_df.dropna(how='all')
    return tags_df