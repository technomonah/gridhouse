-- Business Vault: enriched company view.
-- Combines hub_company + sat_company_details (HH structured data) with
-- company_name extracted from vacancy text by score_vacancies.py (covers TG/LinkedIn).
-- One row per unique company. Structured HH data takes priority over extracted names.

{{
  config(
    materialized='table',
    file_format='iceberg'
  )
}}

WITH latest_company_details AS (
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_company_hk ORDER BY load_dts DESC) AS rn
    FROM {{ ref('sat_company_details') }}
  )
  WHERE rn = 1
),

-- Vacancies linked to a hub_company (HH-sourced)
linked_companies AS (
  SELECT
    hc.hub_company_hk,
    hc.company_name_bk                   AS company_name,
    cd.employer_id,
    cd.area_name,
    'structured'                         AS name_source
  FROM {{ ref('hub_company') }}           hc
  LEFT JOIN latest_company_details        cd ON hc.hub_company_hk = cd.hub_company_hk
),

-- Company names extracted from vacancy text (covers TG/LinkedIn vacancies with no hub_company link)
extracted_companies AS (
  SELECT DISTINCT
    NULL                                 AS hub_company_hk,
    company_name,
    NULL                                 AS employer_id,
    NULL                                 AS area_name,
    'extracted'                          AS name_source
  FROM {{ ref('bv_vacancy_score') }}
  WHERE company_name IS NOT NULL
    -- Exclude names already covered by structured hub_company rows
    AND UPPER(TRIM(company_name)) NOT IN (
      SELECT UPPER(TRIM(company_name)) FROM linked_companies
    )
)

SELECT hub_company_hk, company_name, employer_id, area_name, name_source FROM linked_companies
UNION ALL
SELECT hub_company_hk, company_name, employer_id, area_name, name_source FROM extracted_companies
