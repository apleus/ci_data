import aws_helpers
from dotenv import load_dotenv
import os
import query_helpers


if __name__ == '__main__':
    load_dotenv(dotenv_path='../.env')
    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()

    product_ids = query_helpers.get_product_list()
    for id in product_ids:
        query_file = open('./sql/update_reviews.sql', 'r')
        query = query_file.read()

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


    # for each product...
    # get latest file to upload to rds
    # insert with sql (if product_id / review_id already exists, don't)
    # update pipeline_metadata table