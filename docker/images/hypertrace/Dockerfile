FROM eclipse-temurin:11-jre-noble

ENV PINOT_HOME=/opt/pinot

COPY pinot-distribution/target/apache-pinot-1.2.0-bin/apache-pinot-1.2.0-bin /opt/pinot

EXPOSE 9000 8099 8098 8097 8096

WORKDIR ${PINOT_HOME}

ENTRYPOINT ["./bin/pinot-admin.sh"]
