from datetime import datetime
import os
from pathlib import Path
import re
from typing import List

from dotenv import load_dotenv
import pandas as pd
from pydantic import ValidationError

import utils.aws_s3_rds as aws
from utils.models import ReviewModel
import utils.queries as queries

"""
Sanitizes and validates 'raw' data and saves as 'prep' data
"""

def sanitize_json(json_content):
    """
    sanitize dataframe of reviews data
    - remove separators from name, title, other, body
    - simplify rating str to int
    - trim location str
    - convert date to YYYYMMDD
    - simplify verified str to bool
    TODO EXTENSION: Better way of parsing 'other' ?

    Args:
        json_content (list of dicts): raw reviews
    Returns:
        df (pandas dataframe): dataframe of sanitized reviews
    """
    df = pd.DataFrame.from_dict(json_content, orient='columns')

    df['name'] = df['name'].map(lambda x: x.replace(',', ''))
    df['rating'] = df['rating'].map(lambda x: int(str(x)[0]))
    df['title'] = df['title'].map(lambda x: x.replace(',', ''))
    df['location'] = df['location'].map(lambda x: re.sub(r"[^a-zA-Z ]",
                                                        "",
                                                        x.split("Reviewed in ")[-1].split("the ")[-1]).strip()
                                                        )
    df['date'] = df['date'].map(lambda x: str(datetime.strptime(x,'%B %d, %Y').strftime("%Y%m%d")))
    df['other'] = df['other'].map(lambda x: x.replace(',', ''))
    df['verified'] = df['verified'].map(lambda x: x == "Verified Purchase")
    df['body'] = df['body'].map(lambda x: x.replace(',', ''))
    return df


def validate_df(df):
    """
    Use pydantic model to ensure sanitized data properly structured

    Args:
        df (dataframe): santizied review data
    """
    try:
        reviews: List[ReviewModel] = [ReviewModel(**review) for review in df.to_dict('records')]
    except ValidationError as e:
        print("Pydantic Validation Error...")
        print(e)


if __name__ == "__main__":
    """
    For each new raw reviews file:
    - sanitize data
    - validate w/ pydantic
    - upload as prep data csv
    """
    load_dotenv(dotenv_path=str(Path(__file__).parent.parent.resolve()) + '/.env')
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()

    # connect to rds
    conn = aws.connect_to_rds()
    cursor = conn.cursor()

    pipeline_metadata_updates = ""
    for id in product_ids:

        # get most recent raw reviews json data
        cursor.execute(queries.get_pipeline_metadata(product_id=id, status=1))
        date, review_count = queries.parse_query_result(cursor.fetchall()[0])
        json_content = aws.get_json_from_s3(
            bucket=bucket,
            filename='raw/products/' + id + '/' + id + '-' + date + '-reviews.json'
        )

        # sanitize & validate data
        df = sanitize_json(json_content)
        validate_df(df)

        # upload sanitized and validated data as csv
        aws.upload_df_csv_to_s3(
            bucket=bucket,
            filename='prep/products/' + id + '/' + id + '-' + today + '-reviews.csv',
            df=df
        )

        # craft query to update pipeline_metadata
        pipeline_metadata_updates += queries.update_pipeline_metadata_table(id, today, review_count, 2)
    
    # execute all relevant updates to pipeline_metadata table
    cursor.execute(pipeline_metadata_updates)
    
    # clean up rds connection
    conn.commit()
    cursor.close()
    conn.close()