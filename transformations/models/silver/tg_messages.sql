-- Silver model: normalize and deduplicate Telegram messages.
--
-- Filters to passed_prefilter=true rows only — noise stays in Bronze.
-- Dedup strategy: MD5(LOWER(TRIM(text))) content hash, earliest extracted_at wins.
-- Output written to nessie.silver.tg_messages (Iceberg via Nessie).

MODEL (
  name nessie.silver.tg_messages,
  kind FULL,
  dialect spark,
  table_format iceberg
);

WITH filtered AS (
  -- Keep only prefiltered rows with non-empty text.
  SELECT
    CAST(message_id AS STRING)     AS source_id,
    channel                        AS source_channel,
    text,
    LOWER(TRIM(text))              AS text_normalized,
    MD5(LOWER(TRIM(text)))         AS content_hash,
    published_at,
    extracted_at,
    url
  FROM nessie.bronze.tg_messages
  WHERE passed_prefilter = true
    AND text IS NOT NULL
    AND TRIM(text) != ''
),
deduped AS (
  -- Keep one row per content_hash — earliest extraction timestamp wins.
  -- Handles cross-channel reposts (same message in multiple TG channels).
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY content_hash
        ORDER BY extracted_at ASC
      ) AS rn
    FROM filtered
  )
  WHERE rn = 1
)
SELECT
  source_id,
  'telegram'       AS source,
  source_channel,
  text,
  text_normalized,
  content_hash,
  published_at,
  extracted_at,
  url
FROM deduped
