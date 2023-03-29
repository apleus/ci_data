from datetime import datetime
import os
from pathlib import Path
import re
from typing import List

from dotenv import load_dotenv
import pandas as pd
from pydantic import ValidationError

import utils.aws as aws
from utils.models import ReviewModel
import utils.queries as queries

"""Sanitizes and validates 'raw' data and saves as 'prep' data (csv)."""


def format_location(x):
    """Format location string by isolating country.

    Args:
        x (str): String in the format "Reviewed in the United States[emoji]"
    Return:
        String where country is isolated, e.g. "United States"
    """
    return re.sub(
        r"[^a-zA-Z ]","",x.split("Reviewed in ")[-1].split("the ")[-1]).strip()


def sanitize_json(json_content):
    """Sanitize dataframe of reviews data -- trim, typecast, format, etc.

    Args:
        json_content (list of dicts): Raw reviews
    Returns:
        df (dataframe): Dataframe of sanitized reviews
    """
    df = pd.DataFrame.from_dict(json_content, orient='columns')
    df['name'] = df['name'].map(lambda x: x.replace(',', ''))
    df['rating'] = df['rating'].map(lambda x: int(str(x)[0]))
    df['title'] = df['title'].map(lambda x: x.replace(',', ''))
    df['location'] = df['location'].map(lambda x: format_location(x))
    df['date'] = df['date'].map(
        lambda x: str(datetime.strptime(x,'%B %d, %Y').strftime("%Y%m%d")))
    df['other'] = df['other'].map(lambda x: x.replace(',', ''))
    df['verified'] = df['verified'].map(lambda x: x == "Verified Purchase")
    df['body'] = df['body'].map(lambda x: x.replace(',', ''))
    return df


def validate_df(df):
    """Use Pydantic model to ensure data properly structured.

    Args:
        df (dataframe): Santizied review data.
    """
    try:
        reviews: List[ReviewModel] = [ReviewModel(**review) for 
                                      review in df.to_dict('records')]
    except ValidationError as e:
        print("Pydantic Validation Error...")
        print(e)


def get_past_metadata(db, ids):
    """Gets most recent raw scrape metadata for each product ID.

    Args:
        db (RDSConnection): connection to RDS db.
        ids (list): List of product IDs.
    Returns:
        raw_metadata (list): List of [id, date, review_count] lists.
    """
    raw_metadata = []
    with db.managed_cursor() as cur:
        cur.execute(queries.init_products_table() + queries.init_pipeline_metadata_table())  
        for id in ids:
            cur.execute(queries.get_pipeline_metadata(id, 1))
            date, review_count = queries.parse_query_result(cur.fetchall()[0])
            raw_metadata.append([id, date, review_count])
    return raw_metadata


if __name__ == "__main__":

    load_dotenv(str(Path(__file__).parent.parent.resolve()) + '/.env')
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()
    db = aws.RDSConnection()
    raw_metadata = get_past_metadata(db, product_ids)
 
    db_updates = []
    for id, date, review_count in raw_metadata:
        json_content = aws.get_json_from_s3(
            bucket=bucket,
            filename=f'raw/products/{id}/{id}-{date}-reviews.json'
        )
        df = sanitize_json(json_content)
        validate_df(df)
        aws.upload_df_csv_to_s3(
            bucket=bucket,
            filename=f'prep/products/{id}/{id}-{today}-reviews.csv',
            df=df
        )
        db_updates.append(
            queries.update_pipeline_metadata_table(id, today, review_count, 2))
 
    with db.managed_cursor() as cur:
        cur.execute(''.join(db_updates))