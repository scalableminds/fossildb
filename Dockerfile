FROM eclipse-temurin:21-jammy

RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /fossildb
WORKDIR /fossildb

COPY target/scala-2.13/fossildb.jar .
COPY fossildb .

RUN groupadd -r fossildb \
  && useradd -r -g fossildb fossildb \
  && ln -s /fossildb/fossildb /usr/local/bin \
  && chmod 777 . \
  && chown -R fossildb .

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.20 && \
  wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
  chmod +x /bin/grpc_health_probe

EXPOSE 7155

HEALTHCHECK \
  --interval=2s --timeout=5s --retries=30 \
  CMD /bin/grpc_health_probe -addr=:7155

CMD [ "fossildb" ]
