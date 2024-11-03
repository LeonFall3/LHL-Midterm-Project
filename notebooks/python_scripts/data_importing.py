import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from  dateutil.parser import parse


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

    df = df_tags.set_index('property_id').join(df.set_index('property_id'), how='left')
    df = df.drop(columns='tags')

    #droping unneeded columns
    df=df.drop(['description.sold_date','list_date','last_update_date','branding', 'source','matterport', 'photos', 'virtual_tours','primary_photo.href','source.plan_id','source.agents','source.spec_id','source.type','lead_attributes.show_contact_an_agent','products.brand_name','other_listings.rdc','primary_photo','products','other_listings', 'open_houses'], axis=1)

    # dtypes
    df = df.convert_dtypes()



    return df