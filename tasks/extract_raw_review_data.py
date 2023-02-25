import os
from datetime import datetime
from dotenv import load_dotenv
import aws_helpers
import math

from amazon_reviews import Reviews

from extract_models import ProductModel, PipelineMetadataModel
from pydantic import ValidationError


def update_products_table(product_id, brand, title):
    """
    Sanitizes and validates product data and crafts query
    to update products table with new product data

    Inputs:
    product_id (str): unique product ID
    brand (str): product brand as listed on amazon
    title (str): title of product as listed on amazon
    Returns:
    query: creates products table if necessary, adds
        product data to products table if necessary
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


def init_pipeline_metadata_table(product_id):
    """
    Create query to extract latest review_count for specific product_id
    from pipeline_metadata

    Args:
        product_id (str): unique ID of product
    Returns:
        query: creates pipeline metadata_table if necessary,
            extracts latest review_count for product
    """

    # create query to update pipeline_metadata table
    query_file = open('./sql/init_pipeline_metadata.sql', 'r')
    query = query_file.read()
    query = query.format(
        product_id=product_id
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


def calc_pages_to_scrape(review_count, query_response, reviews_per_page=10):
    """
    Calculates how many review pages to scrape given information on
    current and past review counts

    Args:
        review_count (int): current review count
        query_response (list): list of tuple of most recent past review count
        reviews_per_page (int): number of product reviews per page
    Return:
        page_num (int): number of pages of reviews to scrape
    """
    past_review_count = 0 if query_response == [] else query_response[0][0]
    page_num = math.ceil(float(review_count - past_review_count)/reviews_per_page)
    page_num = 1 # TEMP VALUE FOR TESTING
    return page_num


def scrape_new_reviews(product_id):
    """
    Scrapes new reviews of given amazon product
    - updates database of products with product info
    - updates database of pipeline metadata with scrape progress
    - saves json of review data to s3

    Args:
        product_id (str): unique amazon product ID
    """
    today = datetime.today().strftime('%Y%m%d')
    amazon_product = Reviews(product_id)
    brand, title, review_count = amazon_product.get_product_info()
    review_count = int(review_count.split('total ratings, ')[-1].split(' with reviews')[0].replace(',',''))

    # init connection to rds
    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()
    # update products table if necessary
    cursor.execute(update_products_table(product_id, brand, title))
    # get last review count from pipeline_metadata table and use it to calculate number of pages to scrape
    cursor.execute(init_pipeline_metadata_table(product_id))
    page_num = calc_pages_to_scrape(review_count, cursor.fetchall())
    # update pipeline_metadata table signifying extract initialized
    cursor.execute(update_pipeline_metadata_table(product_id, today, review_count, 1))

    # get recent reviews of product_id
    reviews = amazon_product.parse_pages(page_num)
    aws_helpers.upload_json_to_s3(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename='raw/products/' + product_id + '/' + product_id + '-' + today + '-reviews.json',
        data=reviews)

    # update pipeline_metadata table signifying extract completed
    cursor.execute(update_pipeline_metadata_table(product_id, today, review_count, 2))
    
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')

    # get list of products to track
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]

    # scrape new product reviews
    for product_id in product_ids:
        scrape_new_reviews(product_id)


