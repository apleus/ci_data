# ci_data


Schema notes:

Products table:
PRODUCT_ID | BRAND | PRODUCT TITLE

Reviews table:
PRODUCT_ID | NAME | RATING | TITLE | LOCATION | DATE | OTHER | VERIFIED | BODY

Images table:
PRODUCT_ID | IMAGE1_LINK | ...




Transformations to do:

rating
date = datetime.strptime(date,'%B %d, %Y').strftime("%Y-%m-%d")
separate out 'other' attributes...