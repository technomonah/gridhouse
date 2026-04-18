-- Mart: flat denormalized vacancy table for analysis and reporting.
-- One row per unique vacancy. Satellites joined at their latest load_dts.
-- Score join is LEFT JOIN — unscored vacancies included with NULL score fields.
-- Company join is LEFT JOIN — TG/LinkedIn vacancies without a company link survive.

{{
  config(
    materialized='table',
    file_format='iceberg'
  )
}}

WITH latest_details AS (
  -- Latest version of core vacancy fields (text, url, published_at)
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_vacancy_hk ORDER BY load_dts DESC) AS rn
    FROM {{ ref('sat_vacancy_details') }}
  ) WHERE rn = 1
),
latest_source AS (
  -- Latest source-specific metadata (salary for HH, contact for LinkedIn, channel for TG)
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_vacancy_hk ORDER BY load_dts DESC) AS rn
    FROM {{ ref('sat_vacancy_source') }}
  ) WHERE rn = 1
),
latest_score AS (
  -- Latest AI scoring result. Not a dbt model — written by score_vacancies.py via PyIceberg.
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_vacancy_hk ORDER BY load_dts DESC) AS rn
    FROM {{ source('gold_external', 'sat_vacancy_score') }}
  ) WHERE rn = 1
),
latest_company AS (
  -- Latest company metadata (HH-sourced only)
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY hub_company_hk ORDER BY load_dts DESC) AS rn
    FROM {{ ref('sat_company_details') }}
  ) WHERE rn = 1
)

SELECT
  -- Identity
  hv.hub_vacancy_hk,
  hv.content_hash_bk,
  hv.load_dts                         AS first_seen_at,

  -- Core details (sat_vacancy_details)
  d.source,
  d.url,
  d.text,
  d.published_at,
  d.published_at_approx,
  d.load_dts                          AS extracted_at,

  -- HH-specific fields (sat_vacancy_source)
  s.salary_from,
  s.salary_to,
  s.salary_currency,
  s.salary_gross,
  s.area_name,
  s.experience,
  s.employment,
  s.schedule,

  -- LinkedIn contact fields (sat_vacancy_source)
  s.contact_author,
  s.contact_company,
  s.contact_author_url,

  -- Telegram fields (sat_vacancy_source)
  s.tg_channel,

  -- AI score — NULL when not yet scored (sat_vacancy_score)
  sc.score,
  sc.reasoning,
  sc.recommended_action,
  sc.scored_at,
  sc.model_version,

  -- Company (hub_company + sat_company_details) — NULL for TG/LinkedIn vacancies
  hc.company_name_bk                  AS company_name,
  cd.employer_id,
  cd.area_name                        AS company_area_name

FROM {{ ref('hub_vacancy') }}               hv
INNER JOIN latest_details                   d   ON hv.hub_vacancy_hk = d.hub_vacancy_hk
INNER JOIN latest_source                    s   ON hv.hub_vacancy_hk = s.hub_vacancy_hk
LEFT  JOIN latest_score                     sc  ON hv.hub_vacancy_hk = sc.hub_vacancy_hk
LEFT  JOIN {{ ref('link_vacancy_company') }} lvc ON hv.hub_vacancy_hk = lvc.hub_vacancy_hk
LEFT  JOIN {{ ref('hub_company') }}         hc  ON lvc.hub_company_hk = hc.hub_company_hk
LEFT  JOIN latest_company                   cd  ON hc.hub_company_hk = cd.hub_company_hk
