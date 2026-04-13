-- Gold Data Vault 2.0 — Link: vacancy ↔ company relationship.
--
-- Links hub_vacancy to hub_company through the HH employer_name field.
-- Driving key: hub_vacancy_hk (vacancy drives the relationship).
-- link_hk = MD5(content_hash || '||' || company_name_bk).
--
-- Only HH vacancies carry employer_name — TG and LinkedIn vacancies without
-- a company business key produce no link record.

WITH joined AS (
  SELECT
    v.content_hash,
    UPPER(TRIM(hh.employer_name))                                       AS company_name_bk,
    LEAST(v.extracted_at, hh.extracted_at)                              AS load_dts
  FROM {{ ref('vacancies') }} v
  INNER JOIN {{ ref('hh_vacancies') }} hh
    ON v.content_hash = hh.content_hash
  WHERE hh.employer_name IS NOT NULL
    AND TRIM(hh.employer_name) != ''
),
with_keys AS (
  SELECT
    MD5(CONCAT_WS('||', UPPER(TRIM(content_hash)), company_name_bk))   AS link_hk,
    MD5(UPPER(TRIM(content_hash)))                                      AS hub_vacancy_hk,
    MD5(company_name_bk)                                                AS hub_company_hk,
    load_dts,
    'silver.hh_vacancies'                                               AS rec_src
  FROM joined
),
-- Deduplicate by link_hk — same vacancy+company pair should yield one link record.
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY link_hk ORDER BY load_dts ASC) AS rn
  FROM with_keys
)
SELECT link_hk, hub_vacancy_hk, hub_company_hk, load_dts, rec_src
FROM deduped
WHERE rn = 1
