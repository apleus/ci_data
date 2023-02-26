import os
from datetime import datetime
from dotenv import load_dotenv
import aws_helpers
import math

from amazon_reviews import Reviews

from extract_models import ProductModel, PipelineMetadataModel
from pydantic import ValidationError
import query_helpers


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
    past_review_count = 0 if query_response == [] else query_response[0][1]
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
    cursor.execute(query_helpers.init_products_table())
    cursor.execute(query_helpers.update_products_table(product_id, brand, title))
    # get last review count from pipeline_metadata table and use it to calculate number of pages to scrape
    cursor.execute(query_helpers.init_pipeline_metadata_table())
    cursor.execute(query_helpers.get_pipeline_metadata(product_id=product_id, status=4))
    page_num = calc_pages_to_scrape(review_count, cursor.fetchall())
    # update pipeline_metadata table signifying extract initialized
    cursor.execute(query_helpers.update_pipeline_metadata_table(product_id, today, review_count, 1))

    # get recent reviews of product_id
    reviews = amazon_product.parse_pages(page_num)
    aws_helpers.upload_json_to_s3(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename='raw/products/' + product_id + '/' + product_id + '-' + today + '-reviews.json',
        data=reviews)

    # update pipeline_metadata table signifying extract completed
    cursor.execute(query_helpers.update_pipeline_metadata_table(product_id, today, review_count, 2))
    
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')

    # get list of products to track
    product_ids = query_helpers.get_product_list()

    # scrape new product reviews
    for product_id in product_ids:
        scrape_new_reviews(product_id)


