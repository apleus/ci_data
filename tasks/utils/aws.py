import json
import os
from tempfile import NamedTemporaryFile
from contextlib import contextmanager

import pandas as pd

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import psycopg2
from psycopg2 import InterfaceError, DatabaseError

"""Connections, uploads, and downloads from AWS S3 + RDS."""


class RDSConnection:
    """Connection to RDS database.
    
    Attributes:
        host (str): db host.
        port (str): db port.
        user (str): db username.
        password (str): db password.
    """
    def __init__(self):
        self.host = os.environ['RDS_HOST']
        self.port = os.environ['RDS_PORT']
        self.user = os.environ['RDS_USER']
        self.password = os.environ['RDS_PW']
    
    @contextmanager
    def managed_cursor(self):
        """Fuction to create a managed database cursor.

        Yields:
            cur: cursor for RDS PostgreSQL db.
        """
        rds_conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )
        cur = rds_conn.cursor()
        try:
            yield cur
        finally:
            rds_conn.commit()
            cur.close()
            rds_conn.close()


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