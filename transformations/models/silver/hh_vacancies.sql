-- Silver model: normalize and deduplicate HH.ru vacancies.
--
-- text for hashing = CONCAT_WS(' ', vacancy_name, snippet_req, snippet_resp).
-- This matches the prefilter logic in extractors/common.py and is consistent
-- with how TG/LinkedIn use the text field for content_hash computation.
--
-- Dedup is two-stage:
--   1. rn_id: dedup by vacancy_id (same vacancy fetched by multiple search queries)
--   2. rn_hash: dedup by content_hash (different IDs, identical content — edge case)
--
-- Salary fields are retained as nullable — currency normalization (KZT → USD)
-- is a Gold-layer concern handled in sat_vacancy_details.
--
-- Output written to nessie.silver.hh_vacancies (Iceberg via Nessie).

MODEL (
  name nessie.silver.hh_vacancies,
  kind FULL,
  dialect spark,
  table_format iceberg
);

WITH normalized AS (
  SELECT
    vacancy_id,
    vacancy_url,
    vacancy_name,
    employer_id,
    employer_name,
    salary_from,
    salary_to,
    salary_currency,
    salary_gross,
    area_id,
    area_name,
    experience,
    employment,
    schedule,
    COALESCE(snippet_req,  '')                                AS snippet_req,
    COALESCE(snippet_resp, '')                                AS snippet_resp,
    -- Unified text field: vacancy title + requirements + responsibilities.
    CONCAT_WS(' ', vacancy_name,
      COALESCE(snippet_req,  ''),
      COALESCE(snippet_resp, ''))                             AS text,
    LOWER(TRIM(CONCAT_WS(' ', vacancy_name,
      COALESCE(snippet_req,  ''),
      COALESCE(snippet_resp, ''))))                           AS text_normalized,
    MD5(LOWER(TRIM(CONCAT_WS(' ', vacancy_name,
      COALESCE(snippet_req,  ''),
      COALESCE(snippet_resp, '')))))                          AS content_hash,
    published_at,
    extracted_at,
    search_query
  FROM nessie.bronze.hh_vacancies
  WHERE passed_prefilter = true
),
deduped AS (
  -- Two-stage dedup: first by vacancy_id, then by content_hash.
  -- Both row numbers must be 1 for a row to survive.
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY vacancy_id
        ORDER BY extracted_at ASC
      ) AS rn_id,
      ROW_NUMBER() OVER (
        PARTITION BY content_hash
        ORDER BY extracted_at ASC
      ) AS rn_hash
    FROM normalized
  )
  WHERE rn_id = 1 AND rn_hash = 1
)
SELECT
  vacancy_id                AS source_id,
  'hh'                      AS source,
  vacancy_url               AS url,
  vacancy_name,
  employer_id,
  employer_name,
  salary_from,
  salary_to,
  salary_currency,
  salary_gross,
  area_id,
  area_name,
  experience,
  employment,
  schedule,
  snippet_req,
  snippet_resp,
  text,
  text_normalized,
  content_hash,
  published_at,
  extracted_at,
  search_query              AS source_query
FROM deduped
