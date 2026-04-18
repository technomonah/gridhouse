-- Business Vault: computed vacancy status attributes.
-- Calculates is_active (TTL 30 days from publish date) and days_since_published.
-- Vacancies without any publish date are treated as active — better to over-include
-- than miss a fresh vacancy with missing metadata (same policy as score_vacancies.py).

{{
  config(
    materialized='table',
    file_format='iceberg'
  )
}}

WITH latest_details AS (
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_vacancy_hk ORDER BY load_dts DESC) AS rn
    FROM {{ ref('sat_vacancy_details') }}
  )
  WHERE rn = 1
)

SELECT
  hub_vacancy_hk,
  source,
  -- Use published_at if available, fall back to published_at_approx
  COALESCE(published_at, published_at_approx)                           AS effective_published_at,
  -- is_active: within 30-day TTL, or unknown publish date (treated as active)
  CASE
    WHEN COALESCE(published_at, published_at_approx) IS NULL THEN TRUE
    WHEN DATEDIFF(CURRENT_TIMESTAMP(), COALESCE(published_at, published_at_approx)) <= 30 THEN TRUE
    ELSE FALSE
  END                                                                   AS is_active,
  -- days_since_published: NULL when publish date is unknown
  CASE
    WHEN COALESCE(published_at, published_at_approx) IS NULL THEN NULL
    ELSE DATEDIFF(CURRENT_TIMESTAMP(), COALESCE(published_at, published_at_approx))
  END                                                                   AS days_since_published
FROM latest_details
