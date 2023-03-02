-- TODO (extension): more intelligent / systematic filtering of terms

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
  	'could','would','even','also','only'
)
GROUP BY
  word,
  product_id
ORDER BY w_count DESC