-- Gold Data Vault 2.0 — Satellite: company descriptive metadata.
--
-- Attached to hub_company. Populated from silver.hh_vacancies only — HH is the
-- only source with structured company metadata (employer_id, area).
-- One row per company_name_bk: earliest extracted_at wins.
--
-- employer_id is nullable — HH allows anonymous job postings without a company ID.

WITH companies AS (
  SELECT
    UPPER(TRIM(employer_name))  AS company_name_bk,
    employer_id,
    area_id,
    area_name,
    extracted_at,
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(employer_name))
      ORDER BY extracted_at ASC
    )                           AS rn
  FROM {{ ref('hh_vacancies') }}
  WHERE employer_name IS NOT NULL
    AND TRIM(employer_name) != ''
)
SELECT
  MD5(company_name_bk)  AS hub_company_hk,
  extracted_at          AS load_dts,
  'silver.hh_vacancies' AS rec_src,
  MD5(CONCAT_WS('||',
    COALESCE(employer_id, '^^'),
    COALESCE(area_id,     '^^'),
    COALESCE(area_name,   '^^')
  ))                    AS hash_diff,
  employer_id,
  area_id,
  area_name
FROM companies
WHERE rn = 1
