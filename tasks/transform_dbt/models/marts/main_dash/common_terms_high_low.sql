-- TODO (extension): more intelligent / systematic filtering of terms

SELECT
    product_id,
    word,
    high_low,
    count(*) AS w_count
FROM ( 
  SELECT
    product_id,
    CASE
        WHEN rating > 3 THEN 'high'
        WHEN rating < 3 THEN 'low'
        ELSE 'mid'
    END AS high_low, 
    LOWER(unnest(regexp_matches(body, '(\w{4,})', 'g'))) AS word
  FROM {{ ref('stg__reviews') }}
) word_counts_table
WHERE word NOT IN (
    'coffee','machine','nespresso','keurig',
  	'this','that','with','have','from',
  	'could','would','even','also','only'
) AND high_low != 'mid'
GROUP BY
  product_id,
  word,
  high_low
ORDER BY w_count DESC