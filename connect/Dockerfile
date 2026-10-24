FROM maven:3.9.16-eclipse-temurin-17 AS fetch
ARG DEBEZIUM_VERSION=3.7.0.Final
ARG JMX_EXPORTER_VERSION=1.0.1
RUN mkdir -p /plugins /jmx \
 && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz | tar -xz -C /plugins \
 && curl -fsSL -o /jmx/jmx_prometheus_javaagent.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar

FROM confluentinc/cp-kafka-connect:8.2.4
COPY --from=fetch /plugins /usr/share/confluent-hub-components
COPY --from=fetch /jmx /opt/jmx
COPY jmx.yml /opt/jmx/jmx.yml
