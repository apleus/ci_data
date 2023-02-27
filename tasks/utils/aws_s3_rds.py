import json
import os
from tempfile import NamedTemporaryFile

import pandas as pd

import boto3
from botocore.exceptions import NoCredentialsError
import psycopg2

"""
Helper functions for connections, uploads, and downloads from AWS S3 + RDS
"""

def connect_to_rds():
    """
    Connect to RDS Resource using psycopg2

    Returns:
        s3_conn: S3 Resource connection
    """
    try:
        rds_conn = psycopg2.connect(
            host=os.environ['RDS_HOST'],
            port=os.environ['RDS_PORT'],
            user=os.environ['RDS_USER'],
            password=os.environ['RDS_PW']
        )
        return rds_conn
    except Exception as e:
        raise (e)


def connect_to_s3():
    """
    Connect to AWS S3 Resource using boto3 Session

    Returns:
        s3_conn: S3 Resource connection
    """
    try:
        s3_conn = boto3.resource("s3",
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        )
        return s3_conn
    except NoCredentialsError as e:
        raise (e)


def upload_json_to_s3(bucket, filename, data):
    """
    Upload json data to s3

    Args:
        bucket (str): name of bucket
        filename (str): name of file (typically, raw/products/[id]/[id]-[YYYYMMDD]-reviews.json)
        data (list of dicts): list of dicts data (typically, product reviews)
    """
    s3_conn = connect_to_s3()
    tmp = NamedTemporaryFile(mode="w+")
    json.dump(data, tmp)
    tmp.flush()
    s3_conn.meta.client.upload_file(Filename=tmp.name, Bucket=bucket, Key=filename)
    tmp.close()


def upload_df_csv_to_s3(bucket, filename, df):
    """
    Upload dataframe as csv to s3 with sep = '|'

    Args:
        bucket (str): name of bucket
        filename (str): name of file (typically, prep/products/[id]/[id]-[YYYYMMDD]-reviews.csv)
        df (pandas dataframe): dataframe of reviews
    """
    s3_conn = connect_to_s3()
    tmp = NamedTemporaryFile(mode="w+")
    tmp.flush()
    df.to_csv(path_or_buf=tmp.name, sep=',', index=False)
    s3_conn.meta.client.upload_file(Filename=tmp.name, Bucket=bucket, Key=filename)
    tmp.close()


def get_json_from_s3(bucket, filename):
    """
    Retrieve content from json file in s3

    Args:
        bucket (str): name of bucket
        filename (str): name of file
    Returns:
        json_content (list): json content from specified s3 file
    """
    s3_conn = connect_to_s3()
    obj = s3_conn.Object(bucket, filename)
    body = obj.get()['Body'].read().decode('utf-8')
    json_content = json.loads(body)
    return json_content