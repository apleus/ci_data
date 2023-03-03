-- TODO (extension): more intelligent / systematic filtering of terms


SELECT
  r.product_id,
  r.word,
  r.high_low,
  r.w_count,
  p.brand,
  p.title
FROM (
  SELECT
    product_id,
    word,
    high_low,
    count(*) AS w_count
  FROM ( 
    SELECT
      product_id,
      CASE
          WHEN rating > 3 THEN 'Highly Rated (4-5 stars)'
          WHEN rating < 3 THEN 'Poorly Rated (1-2 stars)'
          ELSE 'mid'
      END AS high_low, 
      LOWER(unnest(regexp_matches(body, '(\w{4,})', 'g'))) AS word
    FROM {{ ref('stg__reviews') }}
  ) word_counts_table
  WHERE word NOT IN (
      'coffee','machine','nespresso','keurig',
      'this','that','with','have','from',
      'could','would','even','also','only','there'
  ) AND high_low != 'mid'
  GROUP BY
    product_id,
    word,
    high_low
  ORDER BY w_count DESC
) r
  LEFT JOIN {{ ref('stg__products') }} p ON r.product_id = p.product_id
