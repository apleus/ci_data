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


def scrape_new_data(id, past_review_count):
    """Scrapes net new reviews and metadata of given amazon product.

    Args:
        id (str): unique amazon product ID
        past_review_count (int): most recent past review count
    Returns:
        brand (str): Product brand as listed on Amazon.
        title (str): Title of product as listed on Amazon.
        review_count (int): Number of reviews for product.
        reviews (list): List of dicts of reviews.
    """
    amazon_product = Reviews(id)
    brand, title, rc = amazon_product.get_product_info()
    review_count = int(
        rc.split('total ratings, ')[-1].split(' with reviews')[0].replace(',',''))
    page_num = calc_pages_to_scrape(review_count, past_review_count)
    reviews = amazon_product.parse_pages(page_num)
    return brand, title, review_count, reviews


def get_past_review_counts(db, ids):
    """Gets most recent past review count for each product ID.

    Args:
        db (RDSConnection): connection to RDS db.
        ids (list): List of product IDs.
    Returns:
        past_counts (list): List of [id, review_count] lists.
    """
    past_counts = []
    with db.managed_cursor() as cur:
        cur.execute(queries.init_products_table() + queries.init_pipeline_metadata_table())  
        for id in ids:
            cur.execute(queries.get_pipeline_metadata(id, 3))
            query_response = cur.fetchall()
            past_review_count = 0 if query_response == [] else query_response[0][1]
            past_counts.append([id, past_review_count])
    return past_counts


if __name__ == "__main__":

    load_dotenv(str(Path(__file__).parent.parent.resolve()) + '/.env')
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()
    db = aws.RDSConnection()
    past_counts = get_past_review_counts(db, product_ids)

    db_updates = []
    for id, past_review_count in past_counts:
        brand, title, review_count, reviews = scrape_new_data(id, past_review_count)
        aws.upload_json_to_s3(
            bucket=os.environ['AWS_BUCKET_REVIEWS'],
            filename=f'raw/products/{id}/{id}-{today}-reviews.json',
            data=reviews
        )
        products_update = queries.update_products_table(id, brand, title)
        metadata_update = queries.update_pipeline_metadata_table(id, today, review_count, 1)
        db_updates.append(products_update + metadata_update)

    with db.managed_cursor() as cur:
        cur.execute(''.join(db_updates))