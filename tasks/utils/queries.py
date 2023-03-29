import os
from pathlib import Path

"""Helper functions to generate queries from raw SQL and parse data."""

query_path = str(Path(__file__).parent.parent.resolve()) + '/rds_sql/'


def get_product_list():
    """Retrieves list of Amazon product IDs to track.

    Returns:
        product_ids (list): List of strings of product IDs.
    """
    product_ids = []
    filename = str(Path(__file__).parent.parent.resolve()) + '/products.txt'
    with open(filename) as f:
        product_ids = [line.rstrip() for line in f]
    return product_ids


def init_products_table():
    """Query that initalizes products table if not exists.

    Returns:
        query: SQL query string.
    """
    query = ""
    with open(query_path + 'init_products.sql', 'r') as f:
        query = f.read()
    return query


def update_products_table(product_id, brand, title):
    """Query to update products table with new product metadata.

    Args:
        product_id (str): Unique product ID.
        brand (str): Product brand as listed on Amazon.
        title (str): Title of product as listed on Amazon.
    Returns:
        query: adds product data to products table if necessary
    """
    brand = brand.replace("'", "")
    title = title.replace("'", "")

    query = ""
    with open(query_path + 'update_products.sql', 'r') as f:
        query = f.read()
    query = query.format(
        product_id=product_id,
        brand=brand,
        title=title
    )
    return query


def init_pipeline_metadata_table():
    """Query to initalize pipeline_metadata table if not exists.

    Returns:
        query: SQL query string.
    """
    query = ""
    with open(query_path + 'init_pipeline_metadata.sql', 'r') as f:
        query = f.read()
    return query


def get_pipeline_metadata(product_id, status):
    """Query to get latest date, review count from pipeline_metadata table.

    Args:
        product_id (str): unique ID of product.
        status (int): 1=raw extracted, 2=prep done, 3=loaded into RDS.
    Returns:
        query: Extracts latest review_count for product.
    """
    query = ""
    with open(query_path + 'get_pipeline_metadata.sql', 'r') as f:
        query = f.read()
    query = query.format(
        product_id=product_id,
        status=status
    )
    return query


def update_pipeline_metadata_table(product_id, date, review_count, status):
    """Query to update pipeline_metadata table with new data.

    Args:
        product_id (str): Unique product ID.
        date (str): "YYYYMMDD".
        review_count (int): Number of reviews for product.
        status (int): 1=raw extracted, 2=prep done, 3=loaded into RDS.
    Returns:
        query: Updates pipeline_metadata with new data.
    """
    query = ""
    with open(query_path + 'update_pipeline_metadata.sql', 'r') as f:
        query = f.read()
    query = query.format(
        product_id=product_id,
        date=date,
        review_count=review_count,
        status=status
    )
    return query


def init_reviews_table():
    """Query that initalizes products table if not exists.

    Returns:
        query: SQL query string.
    """
    query = ""
    with open(query_path + 'init_reviews.sql', 'r') as f:
        query = f.read()
    return query


def update_reviews_table(filename):
    """Query to update reviews table with [filename].csv data.

    Args:
        filename (str): Filepath to sanitized S3 reviews data.
    Returns:
        query: Updates reviews table with new data.
    """
    query = ""
    with open(query_path + 'update_reviews.sql', 'r') as f:
        query = f.read()
    query = query.format(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename=filename,
        region=os.environ['AWS_REGION'],
        access_key=os.environ['AWS_ACCESS_KEY_ID'],
        secret_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
    return query


def parse_query_result(query_result):
    """Use result of sql query to get most recent review count.

    Args:
        query_result (tuple): Presumably (date, review_count).
    Returns:
        date (str): Date of most recent pipeline run.
        review_count (int): Total # reviews after most recent pipeline run.
    """
    date, review_count = ['', 0]
    try:
        date = query_result[0]
        review_count = int(query_result[1])
    except IndexError as e:
        raise(e)
    return date, review_count