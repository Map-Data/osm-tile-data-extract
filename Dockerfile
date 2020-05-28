FROM docker.io/debian:buster-slim

# install dependencies
ARG PG_VERSION=11
ENV PG_VERSION=${PG_VERSION}
RUN apt update
RUN apt install -y --no-install-recommends pipenv python3-setuptools rsync postgresql-${PG_VERSION} \
    osmctools osm2pgsql git libpq-dev gcc python3-dev
RUN apt install -y postgresql-${PG_VERSION}-postgis

RUN pg_dropcluster --stop ${PG_VERSION} main

# add sources
ADD Pipfile Pipfile.lock /app/src/
WORKDIR /app/src
RUN pipenv install --system --deploy --ignore-pipfile
RUN apt purge -y libpq-dev gcc python3-dev
RUN apt autoremove -y

ADD main.py /app/src/
ADD osm_tile_data_extract /app/src/osm_tile_data_extract

# add image metadata
VOLUME /app/tmp
VOLUME /app/out
ENTRYPOINT ["/app/src/main.py", "-w", "/app/tmp", "-o", "/app/out"]
