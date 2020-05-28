FROM docker.io/debian:buster-slim

# install dependencies
ARG PG_VERSION=11
ENV PG_VERSION=${PG_VERSION}
RUN apt update
RUN apt install -y --no-install-recommends python3 python pipenv python3-setuptools python3-dev python-wheel \
    python-setuptools python-pip rsync postgresql-${PG_VERSION} osmctools osm2pgsql git libpq-dev gcc make unzip \
    postgis curl
RUN apt install -y postgresql-${PG_VERSION}-postgis

RUN pg_dropcluster --stop ${PG_VERSION} main

# add dependency sources
RUN git clone -b v1.8.0 https://github.com/mapzen/vector-datasource.git /app/src/vector-datasource
RUN pip install -r /app/src/vector-datasource/requirements.txt
ENV PYTHONPATH=${PYTHONPATH}:/app/src/vector-datasource
RUN apt purge -y python-setuptools python-pip

# add sources
ADD Pipfile Pipfile.lock /app/src/osm_tile_data_extract/
WORKDIR /app/src/osm_tile_data_extract
RUN pipenv install --system --deploy
RUN apt purge -y libpq-dev gcc python3-dev
RUN apt autoremove -y

ADD main.py /app/src/osm_tile_data_extract
ADD osm_tile_data_extract /app/src/osm_tile_data_extract/osm_tile_data_extract

# add image metadata
VOLUME /app/tmp
VOLUME /app/out
ENTRYPOINT ["/app/src/osm_tile_data_extract/main.py", "-w", "/app/tmp", "-o", "/app/out"]
