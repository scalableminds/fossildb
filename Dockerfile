FROM openjdk:8-jdk

RUN mkdir -p /fossildb
WORKDIR /fossildb

COPY target/scala-2.12/fossildb.jar .
COPY fossildb .

RUN groupadd -r fossildb \
  && useradd -r -g fossildb fossildb \
  && ln -s /fossildb/fossildb /usr/local/bin \
  && chown -R fossildb .

USER fossildb

CMD [ "fossildb" ]
