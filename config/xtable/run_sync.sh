#!/bin/sh
# /opt/xtable/config/run_sync.sh
set -e

# Set up standard AWS credentials from environment passed from host
export AWS_ACCESS_KEY_ID="${AWS_ACC_KEY}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_KEY}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# 1. Sync AWS datasets
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
  echo "--- Running AWS Iceberg Sync ---"
  envsubst < /opt/xtable/config/sync_config_aws.yaml > /tmp/rendered_sync_config_aws.yaml
  
  java -jar /opt/xtable/xtable-bundled.jar \
    --datasetConfig /tmp/rendered_sync_config_aws.yaml \
    --hadoopConfig /opt/xtable/config/core-site.xml \
    --icebergCatalogConfig /opt/xtable/config/catalog.yaml
fi

# 2. Sync OCI datasets
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
  echo "--- Running OCI Iceberg Sync ---"
  envsubst < /opt/xtable/config/sync_config_oci.yaml > /tmp/rendered_sync_config_oci.yaml
  
  java -jar /opt/xtable/xtable-bundled.jar \
    --datasetConfig /tmp/rendered_sync_config_oci.yaml \
    --hadoopConfig /opt/xtable/config/core-site.xml \
    --icebergCatalogConfig /opt/xtable/config/catalog_oci.yaml
fi

echo "--- All sync runs completed successfully ---"
