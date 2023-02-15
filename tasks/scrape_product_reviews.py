from amazon_reviews import Reviews
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../.env')
print(os.environ["USER_AGENT"])
# set up data tables and blob storage



# get list of products to track
products = []
with open('products.txt') as f:
    products = [line.rstrip() for line in f]


# amazon_reviews = Reviews(products[0])

# pull reviews

