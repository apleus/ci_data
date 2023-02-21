import os
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
import s3_helpers
    

def s3_json_to_df(product_id, date):
    """
    Get 'raw' product review data scraped on specified date from s3, and convert to
    dataframe for processing and transformation to 'prep' data
    """
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    filename = 'raw/products/' + product_id + '/' + product_id + '-' + date + '-reviews.json'
    json_content = s3_helpers.get_json_from_s3(bucket, filename)
    df = pd.DataFrame.from_dict(json_content, orient='columns')
    return df


def df_to_json_s3():
    """
    Convert cleaned dataframe to json and upload to s3 as 'prep' data
    """
    print("TODO")


def prep_data(metadata):
    """
    For each product, get json file of new reviews from s3, put into dataframe, sanitize data

    Inputs:
    metadata (list): list of [product_id, date] lists to determine which files to pull from s3
    """
    for item in metadata:
        product_id, date = item
        df = s3_json_to_df(product_id, date)
        

        # TODO: FORMAT COLUMNS


def get_raw_metadata():
    """
    
    TODO: REPLACE THIS with proper metadata from scrapes table
    """
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]
    today = datetime.today().strftime('%Y%m%d')
    metadata = []
    for id in product_ids:
        metadata.append([id, today])
    return metadata


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')
    metadata = get_raw_metadata()
    prep_data(metadata)


