#!/usr/bin/env bash
# Valida se o ambiente do desafio esta funcionando de ponta a ponta.
set -uo pipefail

API_KEY="${API_KEY:-desafio-2026}"
ok=0; fail=0

check() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    printf '  \033[32m✓\033[0m %s\n' "$label"; ok=$((ok+1))
  else
    printf '  \033[31m✗\033[0m %s\n' "$label"; fail=$((fail+1))
  fi
}

echo ""
echo "Verificando o ambiente do desafio"
echo "---------------------------------"

check "MinIO respondendo (9000)"        curl -sf http://localhost:9000/minio/health/live
check "Console do MinIO (9001)"          curl -sf -o /dev/null http://localhost:9001
check "Catalogo Iceberg REST (8181)"     curl -sf http://localhost:8181/v1/config?warehouse=s3://lakehouse/warehouse
check "Mock API (8000)"                  curl -sf http://localhost:8000/health
check "Mock API exige API key"           bash -c '[ "$(curl -s -o /dev/null -w %{http_code} http://localhost:8000/events)" = "401" ]'
# A API falha de proposito em ~2% das chamadas (FLAKY_RATE) e limita a 10 req/s.
# Uma unica tentativa acusaria falha num ambiente saudavel: tentamos ate 4 vezes.
api_events() {
  local i
  for i in 1 2 3 4; do
    if curl -sf -H "X-API-Key: ${API_KEY}" \
         'http://localhost:8000/events?page=1&page_size=1' | grep -q event_id; then
      return 0
    fi
    sleep 1.5
  done
  return 1
}
check "Mock API retorna eventos"         api_events
check "Postgres aceitando conexao"       docker compose exec -T postgres pg_isready -U app -d crm
check "crm.customers populada"           bash -c "docker compose exec -T postgres psql -U app -d crm -tAc 'select count(*) from crm.customers' | grep -qE '^[1-9]'"
check "Trino no ar (8080)"               docker compose exec -T trino trino --execute "SELECT 1"
check "Trino enxerga o catalogo lakehouse" bash -c "docker compose exec -T trino trino --execute 'SHOW SCHEMAS FROM lakehouse' | grep -qi information_schema"

echo ""
echo "Teste de escrita ponta a ponta (Spark -> Iceberg -> MinIO -> Trino)"
docker compose exec -T spark spark-sql --silent -e "
  CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke;
  DROP TABLE IF EXISTS lakehouse.smoke.t;
  CREATE TABLE lakehouse.smoke.t (id bigint, txt string) USING iceberg;
  INSERT INTO lakehouse.smoke.t VALUES (1,'ok'),(2,'ok');
" >/dev/null 2>&1
check "Spark escreveu tabela Iceberg"  bash -c "docker compose exec -T spark spark-sql --silent -e 'SELECT count(*) FROM lakehouse.smoke.t' 2>/dev/null | grep -q 2"
check "Trino le a MESMA tabela"        bash -c "docker compose exec -T trino trino --execute 'SELECT count(*) FROM lakehouse.smoke.t' 2>/dev/null | grep -q 2"
docker compose exec -T spark spark-sql --silent -e "DROP TABLE IF EXISTS lakehouse.smoke.t" >/dev/null 2>&1

echo ""
echo "---------------------------------"
if [ "$fail" -eq 0 ]; then
  printf '\033[32mAmbiente pronto: %d/%d verificacoes passaram.\033[0m\n\n' "$ok" "$((ok+fail))"
  echo "  MinIO    http://localhost:9001   (admin / minioadmin)"
  echo "  Mock API http://localhost:8000/docs   (header X-API-Key: ${API_KEY})"
  echo "  Trino    http://localhost:8080"
  echo "  Postgres localhost:5432  (app / app / crm)"
  echo ""
else
  printf '\033[31m%d verificacao(oes) falharam.\033[0m Rode "make logs" para investigar.\n' "$fail"
  echo 'Se o Spark ou o Trino ainda estiverem subindo, aguarde ~30s e rode "make check" de novo.'
  exit 1
fi
