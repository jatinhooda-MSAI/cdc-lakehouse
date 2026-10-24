FROM maven:3.9.16-eclipse-temurin-17 AS fetch
ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.4.2
RUN mkdir /jars && cd /jars \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar

FROM flink:1.20.5-scala_2.12-java17
COPY --from=fetch /jars/ /opt/flink/lib/
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && cp /opt/flink/opt/flink-s3-fs-presto-1.20.5.jar /opt/flink/plugins/s3-fs-presto/
