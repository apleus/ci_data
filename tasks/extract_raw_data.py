from datetime import datetime
import math
import os
from pathlib import Path

from dotenv import load_dotenv

from utils.reviews import Reviews
import utils.aws as aws
import utils.queries as queries

"""Scrape recent Amazon reviews for specified product IDs."""


def calc_pages_to_scrape(review_count, past_review_count, reviews_per_page=10):
    """Calculates num pages to scrape given current and past review counts.

    Args:
        review_count (int): Current review count.
        past_review_count (int): Most recent past review count.
        reviews_per_page (int): Number of product reviews per page.
    Return:
        page_num (int): Number of pages of reviews to scrape.
    """
    page_num = math.ceil(
        float(review_count - past_review_count)/reviews_per_page)
    # page_num = 1 # TEMP VALUE FOR TESTING
    return page_num


def scrape_new_reviews(id, date, past_review_count):
    """Scrapes net new reviews of given amazon product.

    Args:
        id (str): unique amazon product ID
        date (str): YYYYMMDD date of scrape
        past_review_count (int): most recent past review count
    Returns:
        query string that updates data in pipeline_metadata and products tables
    """

    amazon_product = Reviews(id)

    brand, title, rc = amazon_product.get_product_info()
    review_count = int(
        rc.split('total ratings, ')[-1].split(' with reviews')[0].replace(',',''))
    page_num = calc_pages_to_scrape(review_count, past_review_count)

    reviews = amazon_product.parse_pages(page_num)
    aws.upload_json_to_s3(
        bucket=os.environ['AWS_BUCKET_REVIEWS'],
        filename=f'raw/products/{id}/{id}-{date}-reviews.json',
        data=reviews
        )

    products_update = queries.update_products_table(id, brand, title)
    metadata_update = queries.update_pipeline_metadata_table(id, date, review_count, 1)
    return products_update + metadata_update


if __name__ == "__main__":

    dotenv_path=str(Path(__file__).parent.parent.resolve()) + '/.env'
    load_dotenv(dotenv_path)
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()

    # TODO: refactor as context manager to manage conn
    conn = aws.connect_to_rds()
    cursor = conn.cursor()
    cursor.execute(
        queries.init_products_table() + queries.init_pipeline_metadata_table())
    db_updates = []

    for product_id in product_ids:

        # get last review count from pipeline_metadata to calc # pages to scrape
        cursor.execute(queries.get_pipeline_metadata(product_id, 3))
        query_response = cursor.fetchall()
        past_review_count = 0 if query_response == [] else query_response[0][1]
        query = scrape_new_reviews(product_id, today, past_review_count)
        db_updates.append(query)
    
    all_updates = ''.join(db_updates)
    cursor.execute(all_updates)
    conn.commit()
    cursor.close()
    conn.close()