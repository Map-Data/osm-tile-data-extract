import argparse
import os
import subprocess
import time
from typing import Union

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import mercantile
from osm_tile_data_extract.util import *


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

        db_group = parser.add_argument_group(title='PostgreSQL connection parameters',
                                             description='The configured user must be a superuser to create the needed'
                                                         'PostgreSQL extensions (postgis and hstore).')
        db_group.add_argument('--db-host', type=str, default='localhost')
        db_group.add_argument('--db-port', type=int, default=5432)
        db_group.add_argument('--db-user', type=str, default='osm_tile_data_extract')
        db_group.add_argument('--db-password', type=str, default='osm_tile_data_extract')

    def __init__(self, args: argparse.Namespace):
        self.api = ApiClient(args.mapping_url, args.mapping_auth)
        self.tile = mercantile.Tile(args.x, args.y, args.z)

        self.working_dir = args.working_dir
        self.out_dir = args.output_dir

        self.db_host = args.db_host
        self.db_port = args.db_port
        self.db_user = args.db_user
        self.db_password = args.db_password
        self.db_conn = None

    def run(self):
        print('Connecting to PostgreSQL server')
        self.db_conn = psycopg2.connect(f'host={self.db_host} port={self.db_port} dbname=postgres '
                                        f'user={self.db_user} password={self.db_password}')
        self.db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        self._download_pbf()
        self._create_postgres_db()
        self._import_into_postgres()

    def _download_pbf(self):
        print(f'Downloading pbf of {self.tile}')
        dump = self.api.get_planet_dump(self.tile)
        subprocess.run(['wget', '-N', '-nv', '--show-progress', dump['file']], check=True, cwd=self.working_dir)

    def _create_postgres_db(self):
        database = f'osm_tile_data_extract_{self.tile.z}_{self.tile.x}_{self.tile.y}'
        # create new database only if necessary
        with self.db_conn.cursor() as cursor:
            cursor.execute('SELECT datname FROM pg_database;')
            if (database,) not in cursor.fetchall():
                print(f'Creating PostgreSQL database {database}')
                cursor.execute(f'CREATE DATABASE {database} OWNER %s', [self.db_user])

        # reconnect to the correct database
        self.db_conn.close()
        self.db_conn = psycopg2.connect(f'host={self.db_host} port={self.db_port} dbname={database} '
                                        f'user={self.db_user} password={self.db_password}')

        # enable needed postgres extensions
        print('Enabling needed PostgreSQL extensions')
        with self.db_conn.cursor() as cursor:
            cursor.execute('CREATE EXTENSION postgis;')
            cursor.execute('CREATE EXTENSION hstore;')

    def _import_into_postgres(self):
        pass
