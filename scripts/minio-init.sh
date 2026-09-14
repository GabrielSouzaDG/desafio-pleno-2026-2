#!/bin/sh
set -e
echo "==> configurando alias do MinIO"
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "    aguardando MinIO..."; sleep 2
done

for bucket in lakehouse; do
  mc mb --ignore-existing "local/$bucket"
done

# estrutura de zonas
mc mb --ignore-existing local/lakehouse/raw
mc mb --ignore-existing local/lakehouse/warehouse

echo "==> buckets prontos:"
mc ls local
