-- Gold Data Vault 2.0 — Link: vacancy ↔ contact relationship.
--
-- Links hub_vacancy to hub_contact through LinkedIn post authorship.
-- A LinkedIn post author hiring for a role = hiring contact associated with a vacancy.
-- link_hk = MD5(content_hash || '||' || contact_name_bk).
--
-- Currently yields 0 rows — LinkedIn Silver is empty at Phase 3 completion.

WITH joined AS (
  SELECT
    v.content_hash,
    UPPER(TRIM(li.author))                                              AS contact_name_bk,
    LEAST(v.extracted_at, li.extracted_at)                              AS load_dts
  FROM {{ ref('vacancies') }} v
  INNER JOIN {{ ref('linkedin_posts') }} li
    ON v.content_hash = li.content_hash
  WHERE li.author IS NOT NULL
    AND TRIM(li.author) != ''
),
with_keys AS (
  SELECT
    MD5(CONCAT_WS('||', UPPER(TRIM(content_hash)), contact_name_bk))   AS link_hk,
    MD5(UPPER(TRIM(content_hash)))                                      AS hub_vacancy_hk,
    MD5(contact_name_bk)                                                AS hub_contact_hk,
    load_dts,
    'silver.linkedin_posts'                                             AS rec_src
  FROM joined
),
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY link_hk ORDER BY load_dts ASC) AS rn
  FROM with_keys
)
SELECT link_hk, hub_vacancy_hk, hub_contact_hk, load_dts, rec_src
FROM deduped
WHERE rn = 1
