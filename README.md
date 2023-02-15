# ci_data



TOMORROW:

setup dag that
1) connects to minio blob storage
2) scrapes and uploads first two pages of reviews every day
3) connects to postgres db and creates proper tables when new product
4) updates product table when new scrape occurs
5) incorporate update logic for new reviews scraped
6) dag with sensor that updates reviews table with every new file
7) pydantic?



Schema notes:

Products table:
PRODUCT_ID | BRAND | PRODUCT TITLE | DATE SCRAPED | REVIEW COUNT

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