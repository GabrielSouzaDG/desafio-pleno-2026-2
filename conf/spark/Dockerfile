# A imagem tabulario/spark-iceberg acessa o MinIO pelo S3FileIO do Iceberg, que é
# interno ao Iceberg. Ela NÃO traz o hadoop-aws, então `s3a://` falha com
# ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem.
#
# Sem isso o candidato não consegue ler a própria raw zone com Spark — que é a
# Parte 2 inteira do desafio. Este Dockerfile detecta a versão do Hadoop embutida
# na imagem e baixa o hadoop-aws correspondente mais o SDK da AWS compatível.

FROM tabulario/spark-iceberg:latest

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ARG SDK_V1=1.12.262
ARG SDK_V2=2.24.6

RUN set -eux; \
    cd /opt/spark/jars; \
    HV="$(ls | sed -n 's/^hadoop-client-api-\(.*\)\.jar$/\1/p' | head -n1)"; \
    if [ -z "$HV" ]; then HV="$(ls | sed -n 's/^hadoop-common-\(.*\)\.jar$/\1/p' | head -n1)"; fi; \
    echo ">> Hadoop detectado na imagem: ${HV:-NAO ENCONTRADO}"; \
    test -n "$HV"; \
    curl -fsSL -O "${MAVEN}/org/apache/hadoop/hadoop-aws/${HV}/hadoop-aws-${HV}.jar"; \
    case "$HV" in \
      3.3.*) echo ">> hadoop 3.3.x: usando AWS SDK v1 ${SDK_V1}"; \
             curl -fsSL -O "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${SDK_V1}/aws-java-sdk-bundle-${SDK_V1}.jar" ;; \
      3.4.*) echo ">> hadoop 3.4.x: usando AWS SDK v2 ${SDK_V2}"; \
             curl -fsSL -O "${MAVEN}/software/amazon/awssdk/bundle/${SDK_V2}/bundle-${SDK_V2}.jar" ;; \
      *)     echo "!! versao de Hadoop nao mapeada: ${HV}" >&2; exit 1 ;; \
    esac; \
    python3 -c "\
import glob, sys, zipfile; \
jars = glob.glob('/opt/spark/jars/hadoop-aws-*.jar'); \
sys.exit('hadoop-aws nao baixado') if not jars else None; \
alvo = 'org/apache/hadoop/fs/s3a/S3AFileSystem.class'; \
nomes = zipfile.ZipFile(jars[0]).namelist(); \
sys.exit('S3AFileSystem ausente em ' + jars[0]) if alvo not in nomes else None; \
print('>> OK: S3AFileSystem presente em', jars[0])"

# A imagem base tem um defeito conhecido (confirmado rodando de verdade,
# inclusive apos `--no-cache --pull`): o JDK dela (openjdk-17-jre-headless,
# via apt) tem o arquivo lib/modules TRUNCADO em exatamente 20971520 bytes
# (deveria ter ~130MB+). Isso nao aparece em `java -version`, mas quebra
# QUALQUER carregamento de classe de java.util.logging.* via reflexao --
# inclusive o proprio spark-submit, que falha logo no startup com
# "ClassFormatError: Truncated class file" / "Incompatible magic value" ao
# inicializar org.apache.spark.internal.Logging. Como o arquivo tem sempre
# o mesmo tamanho exato mesmo pos rebuild com pull forcado, e um defeito da
# propria imagem tabulario/spark-iceberg:latest publicada, nao algo local
# desta maquina. Correcao: baixar um JDK 17 valido (Temurin) e usa-lo como
# JAVA_HOME em vez do que veio via apt na imagem base.
ARG TEMURIN_JDK_URL=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz

RUN set -eux; \
    mkdir -p /opt/temurin17; \
    curl -fsSL "${TEMURIN_JDK_URL}" | tar -xz -C /opt/temurin17 --strip-components=1; \
    MODULES_SIZE="$(stat -c%s /opt/temurin17/lib/modules)"; \
    echo ">> Temurin lib/modules: ${MODULES_SIZE} bytes"; \
    test "$MODULES_SIZE" -gt 50000000

ENV JAVA_HOME=/opt/temurin17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# pytest para tests/test_transforms.py (make test-transforms). NAO estava
# aqui originalmente -- eu tinha instalado na mao dentro do container
# rodando, e isso nao sobrevive a um `docker compose build`/`make clean`.
# So apareceu ao validar o projeto do zero (make clean && make up):
# test-transforms falhava com "No module named pytest" num container
# recem-criado. Fica no Dockerfile para nao depender de rede no momento
# do teste nem de um passo manual documentado à parte.
RUN pip install --no-cache-dir pytest==8.3.3

# NAO definir WORKDIR aqui: o ENTRYPOINT da imagem base e "./entrypoint.sh",
# um caminho relativo. Mudar o diretorio de trabalho quebra a inicializacao com
# 'stat ./entrypoint.sh: no such file or directory'. Herdamos o WORKDIR da base.
