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
import query_helpers

"""
Sanitizes and validates 'raw' data and saves as 'prep' data
"""

def get_filenames():
    """
    Get raw filenames to sanitize + validate and generate prep filenames to save
    - get product IDs
    - for each product ID...
        - find latest raw file to sanitize + validate
        - generate relevant raw and prep filenames

    Returns:
        filenames (list): list of filename lists [[raw, prep], [raw, prep], ...]
    """
    today = datetime.today().strftime('%Y%m%d')

    product_ids = query_helpers.get_product_list()

    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()

    filenames = []
    for id in product_ids:
        query_file = open('./sql/get_pipeline_metadata.sql', 'r')
        query = query_file.read()
        cursor.execute(query.format(product_id=id, status=2))
        query_result = cursor.fetchall()
        date, review_count = ['','']
        try:
            date = query_result[0][0]
            review_count = query_result[0][1]
        except IndexError as e:
            raise(e)
        raw_filename = 'raw/products/' + id + '/' + id + '-' + date + '-reviews.json'
        prep_filename = 'prep/products/' + id + '/' + id + '-' + today + '-reviews.csv'
        filenames.append([raw_filename, prep_filename, id, date, review_count])
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return filenames


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


if __name__ == "__main__":
    """
    For each new raw reviews file:
    - sanitize data
    - validate w/ pydantic
    - upload as prep data csv
    """
    load_dotenv(dotenv_path='../.env')
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    filenames = get_filenames()

    for product in filenames:
        # get
        raw_filename, prep_filename, id, date, review_count = product
        json_content = aws_helpers.get_json_from_s3(bucket, raw_filename)
        # sanitize
        df = pd.DataFrame.from_dict(json_content, orient='columns')
        df = sanitize_df(df)
        # validate
        data_dict = df.to_dict('records')
        try:
            reviews: List[ReviewModel] = [ReviewModel(**review) for review in data_dict]
        except ValidationError as e:
            print("Pydantic Validation Error...")
            print(e)
        # upload
        aws_helpers.upload_df_csv_to_s3(bucket, prep_filename, df)

        conn = aws_helpers.connect_to_rds()
        cursor = conn.cursor()
        cursor.execute(query_helpers.update_pipeline_metadata_table(id, date, review_count, 3))
        conn.commit()
        cursor.close()
        conn.close()