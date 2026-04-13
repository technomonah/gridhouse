-- Gold Data Vault 2.0 — Satellite: vacancy descriptive details.
--
-- Attached to hub_vacancy. Stores canonical text fields from silver.vacancies.
-- hash_diff covers all descriptive attributes — documents what constitutes a
-- "change" if this model is ever switched to incremental.
--
-- load_dts = extracted_at (when this version was first observed in Silver).

SELECT
  MD5(UPPER(TRIM(content_hash)))  AS hub_vacancy_hk,
  extracted_at                    AS load_dts,
  'silver.vacancies'              AS rec_src,
  MD5(CONCAT_WS('||',
    COALESCE(source,                                   '^^'),
    COALESCE(url,                                      '^^'),
    COALESCE(text_normalized,                          '^^'),
    COALESCE(CAST(published_at AS STRING),             '^^'),
    COALESCE(CAST(published_at_approx AS STRING),      '^^')
  ))                              AS hash_diff,
  source,
  url,
  text,
  text_normalized,
  published_at,
  published_at_approx
FROM {{ ref('vacancies') }}
