import aws_helpers
from dotenv import load_dotenv
import os


if __name__ == '__main__':
    load_dotenv(dotenv_path='../.env')
    conn = aws_helpers.connect_to_rds()
    cursor = conn.cursor()

    query_file = open('./sql/update_reviews.sql', 'r')
    query = query_file.read()
    # query.format(

    # )

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()