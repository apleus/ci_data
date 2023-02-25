import os
from datetime import datetime
from dotenv import load_dotenv
import aws_helpers

from amazon_reviews import Reviews

from extract_models import ProductModel
from pydantic import ValidationError


def update_products_table(product_id, brand, title):
    """
    Sanitizes and validates product data and
    crafts query to update products table with new product data

    Inputs:
    product_id (str): unique product ID
    brand (str): product brand as listed on amazon
    title (str): title of product as listed on amazon
    Returns:
    Query: query to create products table if necessary
        and add product data to products table if necessary
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

# def update_pipeline_metadata_table(product_id, date, review_count):




def scrape_net_new_reviews(product_ids):
    for product_id in product_ids:
        amazon_product = Reviews(product_id)

        brand, title, review_count = amazon_product.get_product_info()
        conn = aws_helpers.connect_to_rds()
        cursor = conn.cursor()
        
        cursor.execute(update_products_table(product_id, brand, title))
        conn.commit()



        # TODO FIX TITLE BUG WITH SINGLE QUOTES

        # initialize scrapes db, check for latest scrape
        # calculate page_num
        # add to scrapes db

        cursor.close()
        conn.close()

        # TODO: figure out how many pages to scrape that are net new
        page_num = 2






        # get recent reviews
        # bucket = os.environ['AWS_BUCKET_REVIEWS']
        # filename = 'raw/products/' + product_id + '/' + product_id + '-' + datetime.today().strftime('%Y%m%d') + '-reviews.json'
        # reviews = amazon_product.parse_pages(page_num)
        # aws_helpers.upload_json_to_s3(bucket, filename, reviews)


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')

    # get list of products to track
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]
    scrape_net_new_reviews(product_ids)


