import json
import os
from tempfile import NamedTemporaryFile

import pandas as pd

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import psycopg2
from psycopg2 import InterfaceError, DatabaseError

"""Connections, uploads, and downloads from AWS S3 + RDS."""

def connect_to_rds():
    """Connect to RDS Resource using psycopg2.

    Returns:
        rds_conn: RDS connection.
    """
    # TODO(): refactor as context manager to manage conn
    try:
        rds_conn = psycopg2.connect(
            host=os.environ['RDS_HOST'],
            port=os.environ['RDS_PORT'],
            user=os.environ['RDS_USER'],
            password=os.environ['RDS_PW']
        )
        return rds_conn
    except (InterfaceError, DatabaseError) as e:
        raise (e)


def connect_to_s3():
    """Connect to AWS S3 Resource using boto3 Session.

    Returns:
        s3_conn: S3 Resource connection.
    """
    # TODO(): refactor as context manager to manage conn
    try:
        s3_conn = boto3.resource("s3",
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        )
        return s3_conn
    except ClientError as e:
        raise (e)


def upload_json_to_s3(bucket, filename, data):
    """Upload json data to S3.

    Args:
        bucket (str): Name of S3 bucket.
        filename (str): Filepath + name in S3 where json is to be saved.
        data ([dicts]): List of dicts of data (typically, product reviews).
    """
    s3_conn = connect_to_s3()
    with NamedTemporaryFile(mode="w+") as tmp:
        json.dump(data, tmp)
        tmp.flush()
        s3_conn.meta.client.upload_file(
            Filename=tmp.name,
            Bucket=bucket,
            Key=filename
        )


def upload_df_csv_to_s3(bucket, filename, df):
    """Upload dataframe as csv to S3.

    Args:
        bucket (str): Name of bucket.
        filename (str): Filepath + name in S3 where csv is to be saved.
        df (pandas dataframe): Dataframe of reviews data.
    """
    s3_conn = connect_to_s3()
    with NamedTemporaryFile(mode="w+") as tmp:
        tmp.flush()
        df.to_csv(path_or_buf=tmp.name, sep=',', index=False)
        s3_conn.meta.client.upload_file(
            Filename=tmp.name,
            Bucket=bucket,
            Key=filename
        )


def get_json_from_s3(bucket, filename):
    """Retrieve content from json file in S3.

    Args:
        bucket (str): Name of bucket.
        filename (str): Filepath + name in S3 where json is saved.
    Returns:
        json_content (list): json content from specified S3 file.
    """
    s3_conn = connect_to_s3()
    obj = s3_conn.Object(bucket, filename)
    body = obj.get()['Body'].read().decode('utf-8')
    json_content = json.loads(body)
    return json_content