#!/usr/bin/env bash
# Diagnostico isolado do acesso do Spark ao MinIO via s3a://.
# Distingue "erro de sintaxe/catalogo" de "erro de credencial".
set -uo pipefail

USER_MINIO="${MINIO_ROOT_USER:-admin}"
PASS_MINIO="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "==> 1. recriando o objeto de teste no MinIO"
docker compose exec -T minio mc alias set local http://localhost:9000 "$USER_MINIO" "$PASS_MINIO" >/dev/null 2>&1
docker compose exec -T minio sh -c \
  "printf '%s\n' '{\"probe\":true,\"id\":1}' '{\"probe\":true,\"id\":2}' | mc pipe local/lakehouse/raw/_diag_s3a/probe.json"
docker compose exec -T minio mc ls local/lakehouse/raw/_diag_s3a/

echo
echo "==> 2. conferindo as configuracoes s3a carregadas pelo Spark"
docker compose exec -T spark spark-sql --silent -e "SET spark.hadoop.fs.s3a.endpoint;" 2>/dev/null | tail -2
docker compose exec -T spark spark-sql --silent -e "SET spark.hadoop.fs.s3a.path.style.access;" 2>/dev/null | tail -2
docker compose exec -T spark spark-sql --silent -e "SET spark.sql.defaultCatalog;" 2>/dev/null | tail -2

echo
echo "==> 3. leitura via VIEW TEMPORARIA (independente do catalogo default)"
docker compose exec -T spark spark-sql -e "
CREATE TEMPORARY VIEW p USING json OPTIONS (path 's3a://lakehouse/raw/_diag_s3a/');
SELECT concat('RESULTADO_VIEW=', count(*)) FROM p;
" 2>&1 | grep -E "RESULTADO_VIEW|Error in query|Exception|Denied|403|refused" | head -5

echo
echo "==> 4. leitura via sintaxe json.\`path\` no catalogo Iceberg (deve falhar — e tudo bem)"
docker compose exec -T spark spark-sql -e \
  "SELECT count(*) FROM json.\`s3a://lakehouse/raw/_diag_s3a/\`;" 2>&1 \
  | grep -E "^[0-9]+$|Error in query|Exception" | head -3

echo
echo "==> 5. a mesma leitura apos trocar para o catalogo de sessao"
docker compose exec -T spark spark-sql -e \
  "USE spark_catalog; SELECT concat('RESULTADO_SESSAO=', count(*)) FROM json.\`s3a://lakehouse/raw/_diag_s3a/\`;" 2>&1 \
  | grep -E "RESULTADO_SESSAO|Error in query|Exception" | head -3

echo
echo "==> 6. limpeza"
docker compose exec -T minio mc rm --recursive --force local/lakehouse/raw/_diag_s3a/ >/dev/null 2>&1
echo "pronto"
echo
echo "Leitura do resultado:"
echo "  RESULTADO_VIEW=2          -> o S3A funciona. Ambiente ok."
echo "  ClassNotFoundException    -> falta a JAR hadoop-aws: rode 'make rebuild-spark'."
echo "  Access Denied / 403       -> credencial errada (fs.s3a.access.key / secret.key)."
echo "  Connection refused        -> endpoint errado (use http://minio:9000, nao localhost)."
echo "  UNSUPPORTED_DATASOURCE... -> spark.sql.defaultCatalog aponta para o catalogo Iceberg;"
echo "                               remova essa linha do spark-defaults.conf."
