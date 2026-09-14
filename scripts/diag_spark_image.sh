#!/usr/bin/env bash
# Diagnostica por que o hadoop-aws nao aparece no Spark depois do rebuild.
set -uo pipefail

echo "==> 1. estado do container"
docker compose ps spark

echo
echo "==> 2. imagem que o container esta usando"
docker compose images spark
echo "   (esperado: desafio-lakehouse/spark-iceberg:local)"

echo
echo "==> 3. a imagem local existe?"
docker images | grep -E "desafio-lakehouse/spark-iceberg|tabulario/spark-iceberg" || echo "   nenhuma das duas encontrada"

echo
echo "==> 4. conteudo de /opt/spark/jars relacionado a AWS/S3"
if docker compose exec -T spark true >/dev/null 2>&1; then
  docker compose exec -T spark sh -c "ls /opt/spark/jars | grep -iE 'aws|s3|bundle' || echo '   nenhuma JAR de AWS/S3'"
  echo
  echo "==> 5. versao do Hadoop embutida"
  docker compose exec -T spark sh -c "ls /opt/spark/jars | grep -E '^hadoop-(client-api|common)-' || echo '   nao identificada'"
else
  echo "   container nao aceita exec — veja os logs abaixo"
  docker compose logs --tail=40 spark
  exit 1
fi

echo
echo "==> 6. o Dockerfile esta no lugar certo?"
if [ -f conf/spark/Dockerfile ]; then
  echo "   conf/spark/Dockerfile OK"
  head -1 conf/spark/Dockerfile
else
  echo "   FALTANDO: conf/spark/Dockerfile"
  echo "   O compose aponta 'build: ./conf/spark'. Sem esse arquivo o build nao faz nada de util."
fi

echo
echo "==> 7. o compose esta mandando construir?"
grep -A3 "^  spark:" docker-compose.yml | grep -E "build:|image:" || echo "   servico spark sem build/image"

echo
echo "Leitura:"
echo "  imagem = tabulario/...      -> o container nao foi recriado: docker compose up -d --force-recreate spark"
echo "  imagem = desafio-lakehouse  mas sem JARs -> o build rodou em cache ou o Dockerfile nao foi aplicado:"
echo "                                 docker compose build --no-cache --progress=plain spark"
echo "  conf/spark/Dockerfile ausente -> copie o arquivo Dockerfile-spark para conf/spark/Dockerfile"
