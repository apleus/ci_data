from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

import utils.aws as aws
import utils.queries as queries

"""
LOAD: Loads sanitized prep data from s3 into RDS reviews table
"""

if __name__ == '__main__':
    dotenv_path=str(Path(__file__).parent.parent.resolve()) + '/.env'
    load_dotenv(dotenv_path)
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()

    # TODO: refactor as context manager to manage conn
    conn = aws.connect_to_rds()
    cursor = conn.cursor()
    cursor.execute(queries.init_reviews_table())

    updates = []
    for id in product_ids:

        # get most recent prep data file
        cursor.execute(queries.get_pipeline_metadata(product_id=id, status=2))
        date, review_count = queries.parse_query_result(cursor.fetchall()[0])
        prep_filename = f'prep/products/{id}/{id}-{date}-reviews.csv'

        review_update = queries.update_reviews_table(prep_filename)
        pm_update = queries.update_pipeline_metadata_table(id, today, review_count, 3)
        
        updates.append(review_update + pm_update)

    all_updates = ''.join(updates)
    cursor.execute(all_updates)
    conn.commit()
    cursor.close()
    conn.close()