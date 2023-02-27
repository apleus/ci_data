from datetime import datetime

from dotenv import load_dotenv

import utils.aws_s3_rds as aws
import utils.queries as queries

"""
LOAD: Loads sanitized prep data from s3 into RDS reviews table
"""

if __name__ == '__main__':
    load_dotenv(dotenv_path='../.env')
    
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()

    conn = aws.connect_to_rds()
    cursor = conn.cursor()
    cursor.execute(queries.init_reviews_table())

    updates = ""
    for id in product_ids:

        # get most recent prep data file
        cursor.execute(queries.get_pipeline_metadata(product_id=id, status=3))
        date, review_count = queries.parse_query_result(cursor.fetchall()[0])
        prep_filename = 'prep/products/' + id + '/' + id + '-' + date + '-reviews.csv'
  
        # generate queries to update reviews table and pipeline_metadata
        review_update = queries.update_reviews_table(prep_filename)
        pm_update = queries.update_pipeline_metadata_table(id, today, review_count, 4)
        
        updates += review_update + pm_update

    cursor.execute(updates)
    conn.commit()
    cursor.close()
    conn.close()