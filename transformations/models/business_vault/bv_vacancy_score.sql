-- Business Vault: latest AI score per vacancy.
-- Wraps gold_external.sat_vacancy_score to expose it as a dbt model for proper lineage.
-- Deduplicates to one row per vacancy using the most recent scored_at timestamp.
-- Includes entity extraction fields (company_name, recruiter_contact) written by score_vacancies.py.

{{
  config(
    materialized='table',
    file_format='iceberg'
  )
}}

SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY hub_vacancy_hk ORDER BY load_dts DESC) AS rn
  FROM {{ source('gold_external', 'sat_vacancy_score') }}
)
WHERE rn = 1
