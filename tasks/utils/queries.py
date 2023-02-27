import os

from pydantic import ValidationError

from models import ProductModel, PipelineMetadataModel

"""
Helper fns to generate queries for products, pipeline_metadata, reviews tables
"""

def get_product_list():
    """
    Get list of product ids to track
    TODO EXTENSION: create more systematic way for scraping relevant product IDs

    Returns:
        product_ids (list): list of strings of product IDs
    """
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]
    return product_ids


def init_products_table():
    """
    Generate query that initalizes products table if it
    doesn't exist

    Returns:
        query: SQL query string
    """
    query_file = open('./sql/init_products.sql', 'r')
    query = query_file.read()
    return query


def update_products_table(product_id, brand, title):
    """
    Sanitizes and validates product data and crafts query
    to update products table with new product data

    Args:
        product_id (str): unique product ID
        brand (str): product brand as listed on amazon
        title (str): title of product as listed on amazon
    Returns:
        query: adds product data to products table if necessary
    """

    # sanitize product data
    brand = brand.replace("'", "")
    title = title.replace("'", "")

    # validate product data
    product = {
            'product_id': product_id,
            'brand': brand,
            'title': title
    }
    try:
        product_model = ProductModel(**product)
    except ValidationError as e:
        print("Pydantic Validation Error...")
        print(e)

    # create query to update products table
    query_file = open('./sql/update_products.sql', 'r')
    query = query_file.read()
    query = query.format(
        product_id=product_id,
        brand=brand,
        title=title
    )
    return query


def init_pipeline_metadata_table():
    """
    Generate query that initalizes pipeline_metadata table if it
    doesn't exist

    Returns:
        query: SQL query string
    """

    # create query to update pipeline_metadata table
    query_file = open('./sql/init_pipeline_metadata.sql', 'r')
    query = query_file.read()
    return query


def get_pipeline_metadata(product_id, status):
    """
    Extract latest date, review_count for specific product_id & status
    from pipeline_metadata

    Args:
        product_id (str): unique ID of product
        status (int): 1=extract initialized, 2=extract completed,
                    3=prep completed, 4=uploaded to RDS
    Returns:
        query: extracts latest review_count for product
    """

    # create query to update pipeline_metadata table
    query_file = open('./sql/get_pipeline_metadata.sql', 'r')
    query = query_file.read()
    query = query.format(
        product_id=product_id,
        status=status
    )
    return query


def update_pipeline_metadata_table(product_id, date, review_count, status):
    """
    Validates pipeline metadata and crafts query to update
    pipeline_metadata table with new data

    Args:
        product_id (str): unique product ID
        date (str): YYYYMMDD
        review_count (int): number of reviews on product
        status (int): 1=extract initialized, 2=extract completed,
                    3=prep completed, 4=uploaded to RDS
    Returns:
        Query: query that updates pipeline_metadata with new data
    """
    
    # validate pipeline metadata
    pipeline_metadata = {
        'product_id': product_id,
        'date': date,
        'review_count': review_count,
        'status': status
    }
    try:
        pipeline_metadata_model = PipelineMetadataModel(**pipeline_metadata)
    except ValidationError as e:
        print("Pydantic Validation Error...")
        print(e)

    # create query to update pipeline_metadata table
    query_file = open('./sql/update_pipeline_metadata.sql', 'r')
    query = query_file.read()
    query = query.format(
        product_id=product_id,
        date=date,
        review_count=review_count,
        status=status
    )
    return query


def init_reviews_table():
    """
    Generate query that initalizes products table if it
    doesn't exist

    Returns:
        query: SQL query string
    """
    query_file = open('./sql/init_reviews.sql', 'r')
    query = query_file.read()
    return query


def update_reviews_table(filename):
    """
    Crafts query to update reviews table with sanitized [filename].csv data

    Args:
        filename (str): file path in s3
    Returns:
        query: query that updates reviews table with new data
    """
    query_file = open('./sql/update_reviews.sql', 'r')
    query = query_file.read()
    query = query.format(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename=filename,
        region=os.environ['AWS_REGION'],
        access_key=os.environ['AWS_ACCESS_KEY_ID'],
        secret_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
    return query


def parse_query_result(query_result):
    """
    Use result of sql query to get most recent raw

    Args:
        query_result (tuple): presumably (date, review_count)
    Returns:
        date (str): date of most recent raw file
        review_count (int): number of total reviews
            when most recent raw file was scraped
    """
    date, review_count = ['', 0]
    try:
        date = query_result[0]
        review_count = int(query_result[1])
    except IndexError as e:
        raise(e)
    return date, review_count