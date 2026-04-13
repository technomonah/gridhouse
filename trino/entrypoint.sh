#!/bin/bash
# Substitute MinIO credentials from env vars into the catalog config template.
# Trino .properties files don't support env var expansion natively.
# envsubst is not available in the trinodb image — using sed instead.
# catalog.config-dir is set to /etc/trino/catalog in config.properties (absolute path).
# Writing the rendered catalog file there directly avoids the data-dir symlink conflict.
sed \
  -e "s|\${MINIO_ROOT_USER}|${MINIO_ROOT_USER}|g" \
  -e "s|\${MINIO_ROOT_PASSWORD}|${MINIO_ROOT_PASSWORD}|g" \
  /etc/trino/catalog/iceberg.properties.template \
  > /etc/trino/catalog/iceberg.properties
exec /usr/lib/trino/bin/run-trino
