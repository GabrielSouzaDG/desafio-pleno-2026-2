#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Diagnostica por que o Trino nao alcanca o iceberg-rest e o postgres.
# Sintoma tipico: "The connection attempt failed" ou
#                 "Error occurred while processing GET request"
# ---------------------------------------------------------------------------
set -uo pipefail

NET="${NET:-lakehouse}"

hr() { printf '%s\n' "------------------------------------------------------------"; }

# Teste de porta TCP portatil: tenta python3, depois bash, depois nc.
# A imagem do Trino nao tem as mesmas ferramentas da imagem do Spark.
tcp_test() {
  local servico="$1" host="$2" porta="$3"
  local cmd='
    if command -v python3 >/dev/null 2>&1; then
      python3 -c "import socket,sys
s=socket.socket(); s.settimeout(3)
sys.exit(0 if s.connect_ex((\"'"$host"'\", '"$porta"')) == 0 else 1)"
    elif command -v nc >/dev/null 2>&1; then
      nc -z -w3 '"$host"' '"$porta"'
    elif command -v bash >/dev/null 2>&1; then
      timeout 3 bash -c "echo > /dev/tcp/'"$host"'/'"$porta"'"
    else
      exit 127
    fi'
  if docker compose exec -T "$servico" sh -c "$cmd" >/dev/null 2>&1; then
    printf '   OK     %s:%s\n' "$host" "$porta"
  else
    local rc=$?
    if [ "$rc" = 127 ]; then
      printf '   ?      %s:%s (sem ferramenta de teste no container)\n' "$host" "$porta"
    else
      printf '   FALHA  %s:%s\n' "$host" "$porta"
    fi
  fi
}

echo
echo "DIAGNOSTICO DE REDE DO TRINO"
hr

echo "==> 1. containers anexados a rede '${NET}'"
if docker network inspect "$NET" >/dev/null 2>&1; then
  docker network inspect "$NET" --format '{{range .Containers}}   {{.Name}}{{"\n"}}{{end}}'
  echo "   esperado: dl-minio dl-iceberg-rest dl-spark dl-trino dl-postgres dl-mock-api"
else
  echo "   rede '${NET}' NAO EXISTE"
fi

echo
echo "==> 2. redes de cada container do projeto"
for svc in trino iceberg-rest postgres minio spark; do
  cid="$(docker compose ps -q "$svc" 2>/dev/null)"
  if [ -z "$cid" ]; then
    printf '   %-13s (container ausente)\n' "$svc"
    continue
  fi
  redes="$(docker inspect "$cid" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}({{$v.IPAddress}}) {{end}}')"
  printf '   %-13s %s\n' "$svc" "${redes:-nenhuma}"
done

echo
echo "==> 3. o Trino resolve os nomes dos outros servicos?"
docker compose exec -T trino sh -c 'getent hosts iceberg-rest postgres minio 2>/dev/null' \
  || echo "   FALHOU: o Trino nao resolve nenhum dos nomes"

echo
echo "==> 4. o Trino abre conexao nas portas?"
tcp_test trino iceberg-rest 8181
tcp_test trino postgres 5432
tcp_test trino minio 9000

echo
echo "==> 5. o MESMO teste a partir do Spark (isola Trino x servicos)"
tcp_test spark iceberg-rest 8181
tcp_test spark postgres 5432

echo
echo "==> 6. erro completo da consulta ao catalogo Iceberg"
hr
docker compose exec -T trino trino --execute \
  "SELECT count(*) FROM lakehouse.information_schema.tables" 2>&1 \
  | grep -viE "org.jline|Unable to create a system terminal" | tail -20

echo
echo "==> 7. erro completo da consulta ao Postgres"
hr
docker compose exec -T trino trino --execute \
  "SELECT count(*) FROM crm.crm.customers" 2>&1 \
  | grep -viE "org.jline|Unable to create a system terminal" | tail -12

echo
echo "==> 8. ultimas linhas do log do Trino"
hr
docker compose logs --tail=25 trino 2>&1 | tail -25

echo
hr
echo "COMO LER O RESULTADO"
echo
echo "  dl-trino ausente em (1), ou sem a rede '${NET}' em (2)"
echo "      -> o container esta fora da rede. Correcao:"
echo "         docker compose up -d --force-recreate trino"
echo
echo "  (4) falha e (5) passa"
echo "      -> problema isolado no Trino. Mesma correcao acima."
echo
echo "  (4) e (5) falham"
echo "      -> a rede do projeto se perdeu. Correcao:"
echo "         docker compose down && docker compose up -d"
echo
echo "  nomes resolvem e portas OK, mas as queries falham"
echo "      -> nao e rede. A causa esta no texto de (6) e (7):"
echo "         'Failed to open transaction' / 404 -> conf/trino/catalog/lakehouse.properties"
echo "         'password authentication failed'   -> credenciais em crm.properties"
echo "         'Catalog does not exist'           -> docker compose restart trino"
echo
