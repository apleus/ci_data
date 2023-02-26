# ci_data

Misc notes...

App flow...

0. Prepare for scrape
- (manually) specify products to watch in products.txt
- extract_raw_review_data.py
    - for each product
        - Initialize products, pipeline_metadata tables if necessary
        - Get last review_count to calculate how many product review pages to scrape
        - Update pipeline_metadata to signify raw scrape started
        - Scrape product reviews and upload to s3 as json
        - Update pipeline_metadata to signify raw scrape complete
- prep_review_data.py
    - for each product
        - find most recent raw data file
        - sanitize review data
        - validate review data
        - Upload sanitized data to s3 as csv
        - Update pipeline_metadata to signify prep complete
- prep_data_to_rds.py
    - for each product

TODOS:

refactor so extract goes like this:
- get products
- connect...
- init admin databases (can you execute multiple queries at once?)
- get review_count info
- do the scraping, return metadata
- update admin databases (can you execute multiple queries at once?)


3. add new reviews to reviews table
3b. update scrapes table to show completed E, L
refactor...
- should you update pipeline_metadata instead of adding new entry?
    - change key to just product_id, date

EVENTUALLY:
- dbt transformations
- d3.js
- Terraform
- airflow
- put onto AWS
- automate EC2, RDS to stop with action tied to budget alert...


Schema notes:

Products table:
PRODUCT_ID | BRAND | PRODUCT TITLE

Scrapes table:
PRODUCT_ID | DATE SCRAPED | REVIEW COUNT | STATUS (init, raw, prep, transform)

Reviews table:
PRODUCT_ID | NAME | RATING | TITLE | LOCATION | DATE | OTHER | VERIFIED | BODY

extenion:

Images table:
PRODUCT_ID | IMAGE1_LINK | ...


Opportunities for extensions:

add stage at the front of the pipeline to scrape and aggregate product ids. deeper analyses across more products


Products:

coffee makers:
B01NBJ2UT5
B09B1TDJTW
B0B5W1YJN1

socks:
B010RWD4GM
B01IDTX30S
B0BBVRM8LR