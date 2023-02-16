# ci_data

Misc notes...

TODOS:

TOMORROW:
- clean newest data file for each product with pydantic (prep)
- connect to aws db (redshift, or RDS?)
- create reviews table and adds new data files to reviews table in aws db
- 

FRIDAY:
- creates / updates scrapes table with successful scrape + creates product table when necessary
- incorporate update logic for page counter by referencing db
- dbt transformations?



Schema notes:

Products table:
PRODUCT_ID | BRAND | PRODUCT TITLE

Scrapes table:
PRODUCT_ID | DATE SCRAPED | REVIEW COUNT

Reviews table:
PRODUCT_ID | NAME | RATING | TITLE | LOCATION | DATE | OTHER | VERIFIED | BODY

Images table:
PRODUCT_ID | IMAGE1_LINK | ...




Transformations to do:

rating
date = datetime.strptime(date,'%B %d, %Y').strftime("%Y-%m-%d")
separate out 'other' attributes...


Extensions:

automate scraping of product ids for deeper analyses


Products:

coffee makers:
B01NBJ2UT5
B09B1TDJTW
B0B5W1YJN1

socks:
B010RWD4GM
B01IDTX30S
B0BBVRM8LR