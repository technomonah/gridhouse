-- Gold Data Vault 2.0 — Hub: unique vacancies identified by content hash.
--
-- Business key: content_hash (MD5 of normalized vacancy text).
-- content_hash is already the cross-source canonical ID produced by Silver dedup.
-- hub_vacancy_hk = MD5(UPPER(TRIM(content_hash))) — hash of the business key.
--
-- Source priority (hh > telegram > linkedin) is already resolved in silver.vacancies —
-- the winning source row is the canonical representation per content_hash.

SELECT
  MD5(UPPER(TRIM(content_hash)))  AS hub_vacancy_hk,
  content_hash                    AS content_hash_bk,
  MIN(extracted_at)               AS load_dts,
  'silver.vacancies'              AS rec_src
FROM {{ ref('vacancies') }}
GROUP BY content_hash
