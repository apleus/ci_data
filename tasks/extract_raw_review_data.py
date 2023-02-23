import os
from datetime import datetime
from dotenv import load_dotenv
import s3_helpers

from amazon_reviews import Reviews


def scrape_net_new_reviews(product_ids):
    for product_id in product_ids:
        amazon_product = Reviews(product_id)

        # TODO: figure out how many pages to scrape that are net new
        page_num = 2

        # # get updated product info
        # product_info = amazon_product.get_product_info()

        # get recent reviews
        bucket = os.environ['AWS_BUCKET_REVIEWS']
        filename = 'raw/products/' + product_id + '/' + product_id + '-' + datetime.today().strftime('%Y%m%d') + '-reviews.json'
        reviews = amazon_product.parse_pages(page_num)
        s3_helpers.upload_json_to_s3(bucket, filename, reviews)


if __name__ == "__main__":
    load_dotenv(dotenv_path='../.env')

    # get list of products to track
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]

    scrape_net_new_reviews(product_ids)


