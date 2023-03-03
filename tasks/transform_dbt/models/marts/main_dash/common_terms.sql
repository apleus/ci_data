-- TODO (extension): more intelligent / systematic filtering of terms

SELECT
  r.product_id,
  r.word,
  r.w_count,
  p.brand,
  p.title
FROM (
  SELECT
      product_id,
      word,
      count(*) AS w_count
  FROM ( 
    SELECT
      product_id,
      LOWER(unnest(regexp_matches(body, '(\w{4,})', 'g'))) AS word
    FROM {{ ref('stg__reviews') }}
  ) word_counts_table
  WHERE word NOT IN (
      'coffee','machine','nespresso','keurig',
      'this','that','with','have','from',
      'could','would','even','also','only','there'
  )
  GROUP BY
    word,
    product_id
  ORDER BY w_count DESC
) r
  LEFT JOIN {{ ref('stg__products') }} p ON r.product_id = p.product_id