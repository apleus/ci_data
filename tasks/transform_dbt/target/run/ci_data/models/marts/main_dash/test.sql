
  create view "postgres"."warehouse"."test__dbt_tmp" as (
    SELECT * FROM lake.products
  );