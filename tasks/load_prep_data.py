from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

import utils.aws as aws
import utils.queries as queries

"""Loads sanitized prep data from s3 into RDS reviews table."""


def get_past_metadata(db, ids):
    """Gets most recent prep metadata for each product ID.

    Args:
        db (RDSConnection): connection to RDS db.
        ids (list): List of product IDs.
    Returns:
        prep_metadata (list): List of [id, date, review_count] lists.
    """
    prep_metadata = []
    with db.managed_cursor() as cur:
        cur.execute(queries.init_products_table() +
                    queries.init_pipeline_metadata_table() +
                    queries.init_reviews_table())  
        for id in ids:
            cur.execute(queries.get_pipeline_metadata(id, 2))
            date, review_count = queries.parse_query_result(cur.fetchall()[0])
            prep_metadata.append([id, date, review_count])
    return prep_metadata


if __name__ == '__main__':
    load_dotenv(str(Path(__file__).parent.parent.resolve()) + '/.env')
    today = datetime.today().strftime('%Y%m%d')
    product_ids = queries.get_product_list()
    db = aws.RDSConnection()
    prep_metadata = get_past_metadata(db, product_ids)

    db_updates = []
    for id, date, review_count in product_ids:
        prep_filename = f'prep/products/{id}/{id}-{date}-reviews.csv'
        review_update = queries.update_reviews_table(prep_filename)
        pm_update = queries.update_pipeline_metadata_table(id, today, review_count, 3)
        db_updates.append(review_update + pm_update)

    with db.managed_cursor() as cur:
        cur.execute(''.join(db_updates))