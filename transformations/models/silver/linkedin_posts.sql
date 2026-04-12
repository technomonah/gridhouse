-- Silver model: normalize and deduplicate LinkedIn hiring posts.
--
-- published_at is always NULL for LinkedIn — MHTML snapshots contain only
-- relative timestamps ("2 ч.", "1 дн.", "3 нед."), not absolute UTC times.
-- published_at_approx is computed by subtracting the parsed offset from extracted_at.
-- Rows with unrecognized published_at_raw format get NULL for published_at_approx.
--
-- Validate actual values before changing regex:
--   SELECT DISTINCT published_at_raw FROM nessie.bronze.linkedin_posts LIMIT 30
--
-- Output written to nessie.silver.linkedin_posts (Iceberg via Nessie).

MODEL (
  name nessie.silver.linkedin_posts,
  kind FULL,
  dialect spark,
  table_format iceberg
);

WITH filtered AS (
  -- Keep only prefiltered rows with non-empty text.
  SELECT
    post_id,
    post_url,
    author,
    author_url,
    company,
    company_url,
    text,
    LOWER(TRIM(text))              AS text_normalized,
    MD5(LOWER(TRIM(text)))         AS content_hash,
    published_at_raw,
    extracted_at,
    query
  FROM nessie.bronze.linkedin_posts
  WHERE passed_prefilter = true
    AND text IS NOT NULL
    AND TRIM(text) != ''
),
with_approx_ts AS (
  -- Parse relative timestamp from published_at_raw into an approximate UTC timestamp.
  -- Supported formats: "N ч." (hours), "N дн." (days), "N нед." (weeks).
  -- All other formats → NULL (do not fail the pipeline).
  SELECT
    *,
    CASE
      WHEN published_at_raw RLIKE '^\\d+ ч\\.$'
        THEN CAST(extracted_at AS TIMESTAMP)
             - MAKE_INTERVAL(0, 0, 0, 0, CAST(REGEXP_EXTRACT(published_at_raw, '^(\\d+)', 1) AS INT), 0, 0)
      WHEN published_at_raw RLIKE '^\\d+ дн\\.$'
        THEN CAST(extracted_at AS TIMESTAMP)
             - MAKE_INTERVAL(0, 0, 0, CAST(REGEXP_EXTRACT(published_at_raw, '^(\\d+)', 1) AS INT), 0, 0, 0)
      WHEN published_at_raw RLIKE '^\\d+ нед\\.$'
        THEN CAST(extracted_at AS TIMESTAMP)
             - MAKE_INTERVAL(0, 0, CAST(REGEXP_EXTRACT(published_at_raw, '^(\\d+)', 1) AS INT), 0, 0, 0, 0)
      ELSE NULL
    END AS published_at_approx
  FROM filtered
),
deduped AS (
  -- Keep one row per content_hash — earliest extraction timestamp wins.
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY content_hash
        ORDER BY extracted_at ASC
      ) AS rn
    FROM with_approx_ts
  )
  WHERE rn = 1
)
SELECT
  post_id                           AS source_id,
  'linkedin'                        AS source,
  author,
  author_url,
  company,
  company_url,
  text,
  text_normalized,
  content_hash,
  CAST(NULL AS TIMESTAMP)           AS published_at,
  published_at_approx,
  extracted_at,
  post_url                          AS url,
  query                             AS source_query
FROM deduped
