import aws_helpers
from dotenv import load_dotenv
import os
import query_helpers
from datetime import datetime


if __name__ == '__main__':
    load_dotenv(dotenv_path='../.env')
    bucket = os.environ['AWS_BUCKET_REVIEWS']
    region = os.environ['AWS_REGION']
    access_key = os.environ['AWS_ACCESS_KEY_ID']
    secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    today = datetime.today().strftime('%Y%m%d')
    product_ids = query_helpers.get_product_list()

    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()
    cursor.execute(query_helpers.init_reviews_table())

    reviews_updates = ""
    for id in product_ids:

        # get most recent prep data file
        cursor.execute(query_helpers.get_pipeline_metadata(product_id=id, status=3))
        date, review_count = query_helpers.parse_query_result(cursor.fetchall()[0])
        prep_filename = 'prep/products/' + id + '/' + id + '-' + date + '-reviews.csv'
        print(prep_filename)

        # insert with sql (if product_id + review_id already exists, don't)
        query_file = open('./sql/update_reviews.sql', 'r')
        query = query_file.read()
        query = query.format(
            bucket=bucket,
            filename=prep_filename,
            region=region,
            access_key=access_key,
            secret_key=secret_key
        )
        reviews_updates += query #+ query_helpers.update_pipeline_metadata_table(id, today, review_count, 4)

    cursor.execute(reviews_updates)
    conn.commit()
    cursor.close()
    conn.close()