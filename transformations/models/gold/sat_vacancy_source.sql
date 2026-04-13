-- Gold Data Vault 2.0 — Satellite: per-source vacancy metadata.
--
-- Captures source-specific fields not available in silver.vacancies common subset:
--   HH:       salary_from/to/currency/gross, area_name, experience, employment, schedule
--   LinkedIn: contact_author, contact_company, contact_company_url, contact_author_url
--   TG:       tg_channel
--
-- Single unified satellite with nullable source-specific columns — a row populates
-- only the fields relevant to its source. The source column distinguishes which set applies.

WITH hh_enriched AS (
  SELECT
    v.content_hash,
    v.extracted_at,
    v.source,
    hh.salary_from,
    hh.salary_to,
    hh.salary_currency,
    hh.salary_gross,
    hh.area_name,
    hh.experience,
    hh.employment,
    hh.schedule,
    CAST(NULL AS STRING)   AS contact_author,
    CAST(NULL AS STRING)   AS contact_company,
    CAST(NULL AS STRING)   AS contact_company_url,
    CAST(NULL AS STRING)   AS contact_author_url,
    CAST(NULL AS STRING)   AS tg_channel
  FROM {{ ref('vacancies') }} v
  INNER JOIN {{ ref('hh_vacancies') }} hh
    ON v.content_hash = hh.content_hash
  WHERE v.source = 'hh'
),
linkedin_enriched AS (
  SELECT
    v.content_hash,
    v.extracted_at,
    v.source,
    CAST(NULL AS BIGINT)   AS salary_from,
    CAST(NULL AS BIGINT)   AS salary_to,
    CAST(NULL AS STRING)   AS salary_currency,
    CAST(NULL AS BOOLEAN)  AS salary_gross,
    CAST(NULL AS STRING)   AS area_name,
    CAST(NULL AS STRING)   AS experience,
    CAST(NULL AS STRING)   AS employment,
    CAST(NULL AS STRING)   AS schedule,
    li.author              AS contact_author,
    li.company             AS contact_company,
    li.company_url         AS contact_company_url,
    li.author_url          AS contact_author_url,
    CAST(NULL AS STRING)   AS tg_channel
  FROM {{ ref('vacancies') }} v
  INNER JOIN {{ ref('linkedin_posts') }} li
    ON v.content_hash = li.content_hash
  WHERE v.source = 'linkedin'
),
tg_enriched AS (
  SELECT
    v.content_hash,
    v.extracted_at,
    v.source,
    CAST(NULL AS BIGINT)   AS salary_from,
    CAST(NULL AS BIGINT)   AS salary_to,
    CAST(NULL AS STRING)   AS salary_currency,
    CAST(NULL AS BOOLEAN)  AS salary_gross,
    CAST(NULL AS STRING)   AS area_name,
    CAST(NULL AS STRING)   AS experience,
    CAST(NULL AS STRING)   AS employment,
    CAST(NULL AS STRING)   AS schedule,
    CAST(NULL AS STRING)   AS contact_author,
    CAST(NULL AS STRING)   AS contact_company,
    CAST(NULL AS STRING)   AS contact_company_url,
    CAST(NULL AS STRING)   AS contact_author_url,
    tg.source_channel      AS tg_channel
  FROM {{ ref('vacancies') }} v
  INNER JOIN {{ ref('tg_messages') }} tg
    ON v.content_hash = tg.content_hash
  WHERE v.source = 'telegram'
),
all_enriched AS (
  SELECT * FROM hh_enriched
  UNION ALL
  SELECT * FROM linkedin_enriched
  UNION ALL
  SELECT * FROM tg_enriched
)
SELECT
  MD5(UPPER(TRIM(content_hash)))  AS hub_vacancy_hk,
  extracted_at                    AS load_dts,
  'silver.*'                      AS rec_src,
  MD5(CONCAT_WS('||',
    COALESCE(source,                              '^^'),
    COALESCE(CAST(salary_from AS STRING),         '^^'),
    COALESCE(CAST(salary_to AS STRING),           '^^'),
    COALESCE(salary_currency,                     '^^'),
    COALESCE(area_name,                           '^^'),
    COALESCE(experience,                          '^^'),
    COALESCE(contact_author,                      '^^'),
    COALESCE(tg_channel,                          '^^')
  ))                              AS hash_diff,
  source,
  salary_from,
  salary_to,
  salary_currency,
  salary_gross,
  area_name,
  experience,
  employment,
  schedule,
  contact_author,
  contact_company,
  contact_company_url,
  contact_author_url,
  tg_channel
FROM all_enriched
