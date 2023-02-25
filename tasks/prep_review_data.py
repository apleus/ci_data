import os
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
import aws_helpers
import re
from extract_models import ReviewModel
from typing import List
from pydantic import ValidationError


"""
Sanitizes and validates 'raw' data and saves as 'prep' data
"""


def s3_json_to_df(product_id, date):
    """
    Get 'raw' product review data scraped on specified date from s3, and convert to
    dataframe for processing and transformation to 'prep' data

    Inputs:
    product_id (str): product id
    date (str): YYYYMMDD of scrape
    Returns:
    df (pandas dataframe): dataframe of reviews
    """
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    filename = 'raw/products/' + product_id + '/' + product_id + '-' + date + '-reviews.json'
    json_content = aws_helpers.get_json_from_s3(bucket, filename)
    df = pd.DataFrame.from_dict(json_content, orient='columns')
    return df


def df_to_csv_s3(product_id, date, df):
    """
    Convert cleaned dataframe to csv and upload to s3 as 'prep' data
    """
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    filename = 'prep/products/' + product_id + '/' + product_id + '-' + date + '-reviews.csv'
    aws_helpers.upload_df_csv_to_s3(bucket, filename, df)


def sanitize_df(df):
    """
    sanitize dataframe of reviews data
    - remove '|' separators from name, title, other, body
    - simplify rating string to int
    - trim location string to just location
    - convert date string to YYYYMMDD
    - simplify verified string to bool

    TODO: Better way of parsing 'other' ?

    Inputs:
    df (pandas dataframe): dataframe of reviews
    Returns:
    df (pandas dataframe): dataframe of sanitized reviews
    """

    df['name'] = df['name'].map(lambda x: x.replace('|', ''))
    df['rating'] = df['rating'].map(lambda x: int(str(x)[0]))
    df['title'] = df['title'].map(lambda x: x.replace('|', ''))
    df['location'] = df['location'].map(lambda x: re.sub(r"[^a-zA-Z ]", "", \
                                                            x.split("Reviewed in ")[-1].split("the ")[-1]).strip())
    df['date'] = df['date'].map(lambda x: str(datetime.strptime(x,'%B %d, %Y').strftime("%Y%m%d")))
    df['other'] = df['other'].map(lambda x: x.replace('|', ''))
    df['verified'] = df['verified'].map(lambda x: x == "Verified Purchase")
    df['body'] = df['body'].map(lambda x: x.replace('|', ''))
    return df


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
    """
    For each raw reviews file...
    - get json file of new reviews from s3 as pandas dataframe
    - sanitize data
    - run pydantic checks
    - upload as prep data csv
    """
    load_dotenv(dotenv_path='../.env')
    metadata = get_raw_metadata()
    for item in metadata:
        product_id, date = item
        df = s3_json_to_df(product_id, date)
        df = sanitize_df(df)
        data_dict = df.to_dict('records')
        try:
            reviews: List[ReviewModel] = [ReviewModel(**review) for review in data_dict]
        except ValidationError as e:
            print("Pydantic Validation Error...")
            print(e)
        df_to_csv_s3(product_id, date, df)

