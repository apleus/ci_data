
  create view "postgres"."warehouse"."stg__reviews__dbt_tmp" as (
    WITH src AS (
    SELECT *
    FROM "postgres"."lake"."reviews"
), temp as (
    SELECT product_id,
        review_id,
        name,
        rating::INT,
        title,
        location,
        date::TIMESTAMP,
        other,
        verified::BOOLEAN,
        body
    FROM src
)
SELECT *
FROM temp
  );