-- Gold Data Vault 2.0 — Hub: unique companies by normalized employer name.
--
-- Business key: UPPER(TRIM(employer_name)) — normalized company name.
-- Sources: silver.hh_vacancies (employer_name) and silver.linkedin_posts (company).
-- Both sources contribute — earliest extracted_at per business key wins.
--
-- NULL or empty employer_name is excluded — anonymous postings have no company BK.

WITH all_companies AS (
  SELECT
    UPPER(TRIM(employer_name))  AS company_name_bk,
    extracted_at,
    'silver.hh_vacancies'       AS rec_src
  FROM {{ ref('hh_vacancies') }}
  WHERE employer_name IS NOT NULL
    AND TRIM(employer_name) != ''

  UNION ALL

  SELECT
    UPPER(TRIM(company))        AS company_name_bk,
    extracted_at,
    'silver.linkedin_posts'     AS rec_src
  FROM {{ ref('linkedin_posts') }}
  WHERE company IS NOT NULL
    AND TRIM(company) != ''
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY company_name_bk
      ORDER BY extracted_at ASC
    ) AS rn
  FROM all_companies
)
SELECT
  MD5(company_name_bk)  AS hub_company_hk,
  company_name_bk,
  extracted_at          AS load_dts,
  rec_src
FROM ranked
WHERE rn = 1
