#! /usr/bin/bash
set +e

virtualenv .pyenv/bin/activate

wget https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf -N

ln -sf planet-latest.osm.pbf extracts/0_0_0.pbf

./generate_extracts.py
