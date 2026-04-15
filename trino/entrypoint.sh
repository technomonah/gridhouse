#!/bin/bash
# Substitute env vars into Trino catalog config templates.
# Trino .properties files don't support env var expansion natively.
# envsubst is not available in the trinodb image — using sed instead.
# catalog.config-dir is set to /etc/trino/catalog in config.properties (absolute path).
# Writing the rendered catalog files there directly avoids the data-dir symlink conflict.

NESSIE_BRANCH=${NESSIE_BRANCH:-main}

# Render iceberg catalog — targets the branch set by NESSIE_BRANCH (default: main)
sed \
  -e "s|\${NESSIE_BRANCH}|${NESSIE_BRANCH}|g" \
  -e "s|\${MINIO_ROOT_USER}|${MINIO_ROOT_USER}|g" \
  -e "s|\${MINIO_ROOT_PASSWORD}|${MINIO_ROOT_PASSWORD}|g" \
  /etc/trino/catalog/iceberg.properties.template \
  > /etc/trino/catalog/iceberg.properties

# Render iceberg_dev catalog — always targets the dev branch for cross-branch SQL queries
sed \
  -e "s|\${MINIO_ROOT_USER}|${MINIO_ROOT_USER}|g" \
  -e "s|\${MINIO_ROOT_PASSWORD}|${MINIO_ROOT_PASSWORD}|g" \
  /etc/trino/catalog/iceberg_dev.properties.template \
  > /etc/trino/catalog/iceberg_dev.properties

exec /usr/lib/trino/bin/run-trino
