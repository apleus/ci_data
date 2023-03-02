select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- every product_id + review_id combo must be unique

SELECT product_id, review_id, COUNT(*)
FROM "postgres"."warehouse"."stg__reviews"
GROUP BY product_id, review_id
HAVING COUNT(*) >= 1
      
    ) dbt_internal_test