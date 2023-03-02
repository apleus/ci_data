
  create view "postgres"."warehouse"."stg__products__dbt_tmp" as (
    WITH src AS (
    SELECT *
    FROM "postgres"."lake"."products"
), temp as (
    SELECT product_id,
        brand,
        title
    FROM src
)
SELECT *
FROM temp
  );