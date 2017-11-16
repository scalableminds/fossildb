FROM openjdk:8-jdk

RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /fossildb
WORKDIR /fossildb

COPY target/scala-2.12/fossildb.jar .
COPY fossildb .

RUN groupadd -r fossildb \
  && useradd -r -g fossildb fossildb \
  && ln -s /fossildb/fossildb /usr/local/bin \
  && chown -R fossildb .

CMD [ "fossildb" ]
