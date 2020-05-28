import argparse
import os
import subprocess
import time
from typing import Union

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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
        self.api = ApiClient(args.mapping_url, args.mapping_auth)
        self.tile = mercantile.Tile(args.x, args.y, args.z)

        self.working_dir = os.path.abspath(args.working_dir)
        self.out_dir = os.path.abspath(args.output_dir)
        self.pbf_file_name = None       # type: Union[None, str]
        self.db_process = None          # type: Union[None, subprocess.Popen]

    def run(self):
        self._download_pbf()
        self._create_postgres_db()
        #self._import_into_postgres()

    def _download_pbf(self):
        print(f'Downloading pbf of {self.tile}')
        dump = self.api.get_planet_dump(self.tile)
        subprocess.run(['wget', '-N', '-nv', '--show-progress', dump['file']], check=True, cwd=self.working_dir)
        self.pbf_file_name = str(dump['file']).rsplit('/')[-1]

    def _create_postgres_db(self):
        # create new postgresql cluster
        db_dir = os.path.join(self.working_dir, 'pg_data')
        subprocess.run(['rm', '-rf', db_dir])
        os.makedirs(db_dir)
        subprocess.run(['pg_createcluster', PG_VERSION, 'main', '--start', '--datadir', db_dir], check=True)
        db_port = subprocess.run(['pg_lsclusters', '--no-header'], check=True, text=True, stdout=subprocess.PIPE)\
            .stdout.split(' ')[2]

        # create database
        print('Creating PostgreSQL database with extensions')
        subprocess.run(['su', 'postgres', '-c', 'echo "CREATE USER tile_data PASSWORD \'tile_data\';" | psql'], check=True,
                       stdout=subprocess.DEVNULL)
        subprocess.run(['su', 'postgres', '-c', 'echo "CREATE DATABASE tile_data with OWNER tile_data;" | psql'],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['su', 'postgres', '-c', 'echo "CREATE EXTENSION postgis;" | psql -d tile_data'],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['su', 'postgres', '-c', 'echo "CREATE EXTENSION hstore;" | psql -d tile_data'],
                       check=True, stdout=subprocess.DEVNULL)

    def _import_into_postgres(self):
        subprocess.run([
            'osm2pgsql', '--slim', '--drop', '--hstore-all', '-C 3000', '-S osm2pgsql.style',
            '-d', self.db_dbname,
            '-P', self.db_port,
            '-U', self.db_user,
            '--number-processes', max(1, os.cpu_count() - 2),
            os.path.join(self.working_dir, self.pbf_file_name)
        ], check=True, cwd='')
