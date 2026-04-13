-- Gold Data Vault 2.0 — Hub: LinkedIn hiring contacts (post authors).
--
-- Business key: UPPER(TRIM(author)) — person's full name as posted on LinkedIn.
-- Source: silver.linkedin_posts only.
--
-- Currently yields 0 rows — LinkedIn Silver is empty at Phase 3 completion.
-- Model is defined now so the Data Vault DAG is complete and will populate
-- automatically when LinkedIn extractor starts producing data.

WITH contacts AS (
  SELECT
    UPPER(TRIM(author))         AS contact_name_bk,
    extracted_at,
    'silver.linkedin_posts'     AS rec_src
  FROM {{ ref('linkedin_posts') }}
  WHERE author IS NOT NULL
    AND TRIM(author) != ''
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY contact_name_bk
      ORDER BY extracted_at ASC
    ) AS rn
  FROM contacts
)
SELECT
  MD5(contact_name_bk)  AS hub_contact_hk,
  contact_name_bk,
  extracted_at          AS load_dts,
  rec_src
FROM ranked
WHERE rn = 1
