# ci_data

Misc notes...

1. Standard library imports
2. - blank line -
3. google sdk imports
4. - blank line -
5. django imports
6. - blank line -
7. your own code imports

App flow...

0. Prepare for scrape
- (manually) specify products to watch in products.txt
- extract_raw_data.py
    - for each product
        - Initialize products, pipeline_metadata tables if necessary
        - Get last review_count to calculate how many product review pages to scrape
        - Update pipeline_metadata to signify raw scrape started
        - Scrape product reviews and upload to s3 as json
        - Update pipeline_metadata to signify raw scrape complete
- extract_prep_data.py
    - for each product
        - find most recent raw data file
        - sanitize review data
        - validate review data
        - Upload sanitized data to s3 as csv
        - Update pipeline_metadata to signify prep complete
- load_prep_data.py
    - for each product
        - find most recent prep data csv file
        - insert csv data into rds
        - update pipeline_metadata to signify load complete


TODOS:

- dbt transformations
- d3.js
- airflow
- terraform / put everything onto AWS
- makefile, etc.
- cleanup -- read entire codebase; organization? init.py? style? logging?
- documentation

eventually:
- automate EC2, RDS to stop with action tied to budget alert...
- refactor...
    - should you update pipeline_metadata instead of adding new entry?
        - change key to just product_id, date


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