import argparse
import os
import subprocess
import importlib
from typing import Union

import mercantile
from osm_tile_data_extract.util import *

PG_VERSION = os.environ.get('PG_VERSION')


class Program:
    @staticmethod
    def add_args(parser: argparse.ArgumentParser):
        def auth_type(raw: str) -> list:
            if ':' not in raw or len(raw.split(':')) != 2:
                raise argparse.ArgumentTypeError('Authentication has invalid format')
            return raw.split(':')

        parser.add_argument('--mapping-url', dest='mapping_url', type=str, required=True,
                            help='Base URL under which a tileserver-mapping server is reachable')
        parser.add_argument('--mapping-auth', dest='mapping_auth', type=auth_type, required=True,
                            help='<username>:<password> combination used to authenticate at the tileserver-mapping')
        parser.add_argument('-x', type=int, required=True,
                            help='X coordinate of tile to import')
        parser.add_argument('-y', type=int, required=True,
                            help='y coordinate of tile to import')
        parser.add_argument('-z', type=int, required=True,
                            help='z coordinate of tile to import')

    def __init__(self, args: argparse.Namespace):
        self.api = ApiClient(args.mapping_url, args.mapping_auth[0], args.mapping_auth[1])
        self.tile = mercantile.Tile(args.x, args.y, args.z)

        self.working_dir = os.path.abspath(args.working_dir)
        self.out_dir = os.path.abspath(args.output_dir)
        self.pbf_file_name = None  # type: Union[None, str]

    @property
    def db_dbname(self) -> str:
        return f'tile_data_{self.tile.x}_{self.tile.y}_{self.tile.z}'

    @property
    def db_port(self) -> str:
        return subprocess.run(['pg_lsclusters', '--no-header'], check=True, text=True, stdout=subprocess.PIPE) \
            .stdout.split(' ')[2]

    def run(self):
        self._download_pbf()
        self._create_postgres_db()
        self._download_shapefiles()
        self._import_into_postgres()
        self._post_import()
        self._dump_postgres_db()
        subprocess.run(['pg_ctlcluster', PG_VERSION, 'main', 'stop'], check=True)
        self._upload_dump()

    def _download_pbf(self):
        print_stage(f'Downloading pbf of {self.tile}')
        dump = self.api.get_planet_dump(self.tile)
        subprocess.run(['wget', '-N', dump['file']], check=True, cwd=self.working_dir)
        self.pbf_file_name = str(dump['file']).rsplit('/')[-1]

    def _create_postgres_db(self):
        # create new postgresql cluster
        print_stage('Creating PostgreSQL cluster')
        db_dir = os.path.join(self.working_dir, 'pg_data')
        subprocess.run(['rm', '-rf', db_dir], check=True)
        os.makedirs(db_dir)
        subprocess.run(['pg_createcluster', PG_VERSION, 'main', '--start', '--datadir', db_dir], check=True,
                       stdout=subprocess.DEVNULL)

        # create database
        print_stage('Creating PostgreSQL database with extensions')
        subprocess.run(['su', 'postgres', '-c', 'echo "CREATE USER tile_data PASSWORD \'tile_data\';" | psql'],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run(
            ['su', 'postgres', '-c', f'echo "CREATE DATABASE {self.db_dbname} OWNER tile_data;" | psql'],
            check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['su', 'postgres', '-c', f'echo "CREATE EXTENSION postgis;" | psql -d {self.db_dbname}'],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['su', 'postgres', '-c', f'echo "CREATE EXTENSION hstore;" | psql -d {self.db_dbname}'],
                       check=True, stdout=subprocess.DEVNULL)

    def _download_shapefiles(self):
        print_stage('Downloading shapefiles')
        bootstrap_dir = os.path.join(self.working_dir, 'vector-bootstrap')
        os.makedirs(bootstrap_dir, exist_ok=True)
        subprocess.run(['python', './bootstrap.py'], cwd='/app/src/vector-datasource/data', check=True)
        subprocess.run(['make', '-f', '/app/src/vector-datasource/data/Makefile-import-data'],
                       check=True, cwd=bootstrap_dir)

    def _import_into_postgres(self):
        print_stage("Importing data with osm2pgsql")
        subprocess.run([
            'su', 'postgres', '-c',
            f'osm2pgsql --slim --hstore-all -C 3000 '
            f'-S /app/src/vector-datasource/osm2pgsql.style '
            f'-d {self.db_dbname} '
            f'-P {self.db_port} '
            f'-U postgres '
            f'--number-processes {max(1, os.cpu_count() - 2)} '
            f'{os.path.join(self.working_dir, self.pbf_file_name)}'
        ], check=True)

    def _post_import(self):
        print_stage('Importing shapefiles')
        subprocess.run(['su', 'postgres', '-c', f'./import-shapefiles.sh | psql -d {self.db_dbname}'],
                       check=True, cwd=os.path.join(self.working_dir, 'vector-bootstrap'))

        print_stage('Processing imported data')
        subprocess.run(['su', 'postgres', '-c', f'./perform-sql-updates.sh -d {self.db_dbname}'],
                       check=True, cwd='/app/src/vector-datasource/data')

    def _dump_postgres_db(self):
        print_stage('Dumping PostgreSQL database')
        file_path = os.path.join(self.working_dir, 'db.pg_dump')
        with open(file_path, 'wb') as f:
            subprocess.run(['su', 'postgres', '-c',
                            f'pg_dump -p {self.db_port} -d {self.db_dbname} --format custom'],
                           check=True, cwd=self.out_dir, stdout=f)
        subprocess.run(['chown', '0:0', file_path])

    def _upload_dump(self):
        print_stage('Uploading PostgreSQL dump to tileserver-mapping')
        file_path = os.path.join(self.working_dir, 'db.pg_dump')
        self.api.upload_sql_dump(self.tile, file_path)
        subprocess.run(['rsync', file_path, self.out_dir], check=True)
