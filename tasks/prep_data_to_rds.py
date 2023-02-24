import psycopg2
from dotenv import load_dotenv
import os


def connect_to_rds():
    try:
        conn = psycopg2.connect(
            host=os.environ['RDS_HOST'],
            port=os.environ['RDS_PORT'],
            user=os.environ['RDS_USER'],
            password=os.environ['RDS_PW']
        )
        return conn
    except Exception as e:
        print(e)


if __name__ == '__main__':
    load_dotenv(dotenv_path='../.env')
    conn = connect_to_rds()
    cursor = conn.cursor()

    query_file = open('./sql/update_reviews.sql', 'r')
    query = query_file.read()
    # query.format(

    # )

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()