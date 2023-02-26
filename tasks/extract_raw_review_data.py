import os
from datetime import datetime
from dotenv import load_dotenv
import aws_helpers
import math

from amazon_reviews import Reviews

from extract_models import ProductModel, PipelineMetadataModel
from pydantic import ValidationError
import query_helpers


def calc_pages_to_scrape(review_count, past_review_count, reviews_per_page=10):
    """
    Calculates how many review pages to scrape given information on
    current and past review counts

    Args:
        review_count (int): current review count
        past_review_count (int): most recent past review count
        reviews_per_page (int): number of product reviews per page
    Return:
        page_num (int): number of pages of reviews to scrape
    """
    page_num = math.ceil(float(review_count - past_review_count)/reviews_per_page)
    page_num = 1 # TEMP VALUE FOR TESTING
    return page_num


def scrape_new_reviews(product_id, date, past_review_count):
    """
    Scrapes net new reviews of given amazon product

    Args:
        product_id (str): unique amazon product ID
        date (str): YYYYMMDD date of scrape
        past_review_count (int): most recent past review count
    Returns:
        query string that updates data in pipeline_metadata and products tables
    """

    amazon_product = Reviews(product_id)

    # get product info
    brand, title, review_count = amazon_product.get_product_info()
    review_count = int(review_count.split('total ratings, ')[-1].split(' with reviews')[0].replace(',',''))
    page_num = calc_pages_to_scrape(review_count, past_review_count)

    # get recent reviews of product_id
    reviews = amazon_product.parse_pages(page_num)

    # save data to s3
    aws_helpers.upload_json_to_s3(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename='raw/products/' + product_id + '/' + product_id + '-' + date + '-reviews.json',
        data=reviews)

    # craft and return queries to update products + pipeline_metadata tables
    products_update = query_helpers.update_products_table(product_id, brand, title)
    pipeline_metadata_update = query_helpers.update_pipeline_metadata_table(product_id, date, review_count, 2)
    return products_update + pipeline_metadata_update


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')

    # get list of products to track
    product_ids = query_helpers.get_product_list()

    # init connection to rds
    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()

    # init products + pipeline_metadata tables
    cursor.execute(query_helpers.init_products_table() + query_helpers.init_pipeline_metadata_table())

    db_updates = ""
    today = datetime.today().strftime('%Y%m%d')
    # scrape new product reviews
    for product_id in product_ids:

        # get last review count from pipeline_metadata table and use it to calculate number of pages to scrape
        cursor.execute(query_helpers.get_pipeline_metadata(product_id=product_id, status=4))
        query_response = cursor.fetchall()
        past_review_count = 0 if query_response == [] else query_response[0][1]

        # scrape net new reviews for product_id and get query for updates to pipeline_metadata and products tables
        query = scrape_new_reviews(product_id, today, past_review_count)
        db_updates += query
    
    # execute all relevant updates to pipeline_metadata and products tables
    cursor.execute(db_updates)

    # clean up rds connection
    conn.commit()
    cursor.close()
    conn.close()