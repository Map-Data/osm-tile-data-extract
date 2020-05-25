FROM docker.io/debian:buster-slim

# install dependencies
RUN apt update
RUN apt install -y --no-install-recommends pipenv osmctools rsync

# add sources
ADD Pipfile Pipfile.lock /app/src/
WORKDIR /app/src
RUN pipenv install --system --deploy --ignore-pipfile
ADD generate_extracts.py /app/src/

# add image metadata
VOLUME /app/tmp
VOLUME /app/out
ENTRYPOINT ["/app/src/generate_extracts.py", "-w", "/app/tmp", "-o", "/app/out"]
