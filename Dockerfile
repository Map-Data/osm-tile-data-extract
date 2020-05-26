FROM docker.io/debian:buster-slim

# install dependencies
RUN apt update
RUN apt install -y --no-install-recommends pipenv osmctools rsync

# add sources
ADD Pipfile Pipfile.lock /app/src/
WORKDIR /app/src
RUN pipenv install --system --deploy --ignore-pipfile
ADD osm_tile_data_extract /app/

# add image metadata
VOLUME /app/tmp
VOLUME /app/out
ENTRYPOINT ["/app/src/main.py", "-w", "/app/tmp", "-o", "/app/out"]
