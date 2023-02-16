import os
from datetime import datetime
from dotenv import load_dotenv
from tempfile import NamedTemporaryFile
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from amazon_reviews import Reviews

load_dotenv(dotenv_path='../.env')

def connect_to_s3():
    try:
        s3_conn = boto3.resource("s3",
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        )
        return s3_conn
    except NoCredentialsError as e:
        raise (e)


def upload_json_to_s3(product_id, results):
    s3_conn = connect_to_s3()

    # products/[id]/[id]-[YYYMMDD]-reviews.json
    filename = 'raw/products/' + product_id + '/' + product_id + '-' + datetime.today().strftime('%Y%m%d') + '-reviews.json'
    tmp = NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        json.dump(results, f)
        s3_conn.meta.client.upload_file(Filename=tmp.name, Bucket='ci-product-reviews-1', Key=filename)


if __name__ == "__main__":
    # get list of products to track
    product_ids = []
    with open('products.txt') as f:
        product_ids = [line.rstrip() for line in f]

    for product_id in product_ids:
        amazon_product = Reviews(product_id)

        # TODO: figure out how many pages to scrape
        page_num = 2

        # # get updated product info
        # product_info = amazon_product.get_product_info()

        # get recent reviews
        results = amazon_product.compile_all_pages(page_num)

        upload_json_to_s3(product_id, results)





# set up data tables and blob storage






# amazon_reviews = Reviews(products[0])

# pull reviews

