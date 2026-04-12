-- Silver unified model: cross-source dedup of all vacancy sources.
--
-- Unions silver.tg_messages, silver.linkedin_posts, silver.hh_vacancies
-- then removes exact duplicates by content_hash (same text appearing in
-- multiple sources — e.g., a TG post copied verbatim to LinkedIn).
--
-- Source priority when same content_hash appears in multiple sources:
--   hh (1) > telegram (2) > linkedin (3)
-- HH wins because it carries the richest structured metadata (salary, employer_id,
-- area, experience) — this source record flows into Gold Data Vault satellites.
-- Within the same source, earliest extracted_at wins.
--
-- Column contract: common subset only.
-- Source-specific fields (salary, employer, author) are NOT in this table —
-- Gold Data Vault satellites read them from per-source Silver tables directly.
--
-- Output written to nessie.silver.vacancies (Iceberg via Nessie).

MODEL (
  name nessie.silver.vacancies,
  kind FULL,
  dialect spark,
  table_format iceberg,
  depends_on [
    nessie.silver.tg_messages,
    nessie.silver.linkedin_posts,
    nessie.silver.hh_vacancies
  ]
);

WITH tg AS (
  SELECT
    source_id,
    source,
    url,
    text,
    text_normalized,
    content_hash,
    published_at,
    CAST(NULL AS TIMESTAMP)   AS published_at_approx,
    extracted_at
  FROM nessie.silver.tg_messages
),
linkedin AS (
  SELECT
    source_id,
    source,
    url,
    text,
    text_normalized,
    content_hash,
    published_at,
    published_at_approx,
    extracted_at
  FROM nessie.silver.linkedin_posts
),
hh AS (
  SELECT
    source_id,
    source,
    url,
    text,
    text_normalized,
    content_hash,
    published_at,
    CAST(NULL AS TIMESTAMP)   AS published_at_approx,
    extracted_at
  FROM nessie.silver.hh_vacancies
),
all_sources AS (
  SELECT * FROM tg
  UNION ALL
  SELECT * FROM linkedin
  UNION ALL
  SELECT * FROM hh
),
cross_deduped AS (
  -- Cross-source dedup: one row per content_hash.
  -- Tie-breaking: source priority (hh > telegram > linkedin), then earliest extracted_at.
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY content_hash
        ORDER BY
          CASE source
            WHEN 'hh'       THEN 1
            WHEN 'telegram' THEN 2
            ELSE                 3
          END,
          extracted_at ASC
      ) AS rn
    FROM all_sources
  )
  WHERE rn = 1
)
SELECT
  source_id,
  source,
  url,
  text,
  text_normalized,
  content_hash,
  published_at,
  published_at_approx,
  extracted_at
FROM cross_deduped
