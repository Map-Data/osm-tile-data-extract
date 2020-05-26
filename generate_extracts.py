#! /usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
import requests
import json
import mercantile

from requests.auth import HTTPBasicAuth
from multiprocessing import Lock
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_error(m: str):
    print(f'{Colors.FAIL}{m}{Colors.ENDC}', file=sys.stderr)


class Program:
    @staticmethod
    def parse_args() -> argparse.Namespace:
        def directory_type(raw: str):
            p = Path(raw)
            p.mkdir(exist_ok=True)
            if not p.is_dir():
                raise argparse.ArgumentTypeError(f'Path {raw} is not a directory')
            return p

        def auth_type(raw: str) -> list:
            if raw != '' and ':' not in raw and len(raw.split(':')) != 2:
                raise argparse.ArgumentTypeError(f'Authentication has incorrect format. Use <username>:<password>')
            return raw.split(':')

        parser = argparse.ArgumentParser('generate_extracts',
                                         description='Extract similarly sized files from the latest OpenStreetMap '
                                                     'Planet dump.')
        parser.add_argument('-p', '--planet-dump', dest='planet_dump',
                            default='https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf',
                            help='Url of the source pbf file')
        parser.add_argument('-w', '--working-dir', dest='working_dir', type=directory_type,
                            default=os.path.join(os.path.dirname(__file__), 'tmp'),
                            help='Working directory to which the planet dump gets downloaded and in which intermediate '
                                 'split files are stored.')
        parser.add_argument('-o', '--output-dir', dest='output_dir', type=directory_type,
                            default=os.path.join(os.path.dirname(__file__), 'out'))
        parser.add_argument('-s', '--target-size', dest='target_size', default=1.5 * 10 ** 9, type=int,
                            help='Target files will not be larger than this size in bytes')
        parser.add_argument('-z', '--max-zoom', dest='max_zoom', default=9, type=int,
                            help='Maximum zoom level above which no further splitting will be performed')
        parser.add_argument('--processes', default=(max(1, os.cpu_count() - 2)), type=int,
                            help='How many concurrent processes to use')

        upload_group = parser.add_argument_group(title='Uploading',
                                                 description='Finished PBF extracts can be uploaded to a '
                                                             'tileserver-mapping. Use these arguments to do so and '
                                                             'configure how.')
        upload_group.add_argument('--upload-url', dest='upload_url', type=str, default='',
                                  help='Upload to the tileserver-mapping server located at under this url.')
        upload_group.add_argument('--upload-auth', dest='upload_auth', type=auth_type, default='',
                                  help='<username>:<password> combination used to authenticate the upload.')

        return parser.parse_args()

    def __init__(self):
        args = self.parse_args()
        self.working_dir = args.working_dir
        self.out_dir = args.output_dir
        self.target_size = args.target_size
        self.max_zoom = args.max_zoom
        self.planet_dump_url = args.planet_dump

        self.running_futures = 0
        self.lock_running_futures = Lock()

        self.upload_url = args.upload_url
        self.upload_auth = args.upload_auth

        self.executor = ThreadPoolExecutor(max_workers=args.processes)

    @property
    def should_upload(self):
        return self.upload_url != '' and self.upload_auth != ''

    def run(self):
        self.download_planet_dump()
        print('Extracting tiles')
        self.extract(mercantile.Tile(0, 0, 0))

        while self.running_futures > 0:
            time.sleep(10)
        self.executor.shutdown(False)

    def download_planet_dump(self):
        print(f'Downloading {self.planet_dump_url}')
        subprocess.run(['wget', '-N', '-nv', '--show-progress', self.planet_dump_url],
                       check=True, cwd=str(self.working_dir))

        file_name = self.planet_dump_url.rsplit('/', 1)[-1]
        subprocess.run(['ln', '-sf',
                        (self.working_dir / file_name).absolute(),
                        (self.working_dir / '0_0_0.pbf').absolute()
                        ])

    def _generate_tile(self, tile: mercantile.Tile):
        """
        Generate a single target tile from its parent by running osmconvert.

        If the tile is smaller than the intended target size it is considered done and moved to the out_dir.
        If not, additional jobs are scheduled to further break it down.

        If uploading is configured, the results get uploaded to tileserver-mapping as well.

        :param tile: Target tile which should be generated
        """

        box = mercantile.bounds(tile)
        parent = mercantile.parent(tile)

        parent_file = self.working_dir / f'{parent.z}_{parent.x}_{parent.y}.pbf'
        target_file = self.working_dir / f'{tile.z}_{tile.x}_{tile.y}.pbf'

        # these cases should not be hit but we check them regardless
        if not parent_file.exists():
            print_error(f'Not generating {tile} because parent does not exist')
            return
        if parent_file.stat().st_size < self.target_size:
            print_error(f'Not generating {tile} because parent has reached target size')
            return

        if not target_file.exists() or parent_file.stat().st_mtime > target_file.stat().st_mtime:
            # only build file if it does not exist
            # or parent file has been modified since target was last generated
            print(f'Generating {tile}')
            cmd = [
                'osmconvert',
                f'-b={box.west},{box.south},{box.east},{box.north}',
                f'-o={target_file.absolute()}',
                '--complete-ways',
                '--complex-ways',
                '--out-pbf',
                str(parent_file.absolute())
            ]
            subprocess.run(cmd, cwd=str(parent_file.parent), check=True)
        else:
            print(f'{tile} already exists and is current. skipping')

        if target_file.stat().st_size < self.target_size:
            print(f'{Colors.OKGREEN}{tile} has reached target size{Colors.ENDC}')
            self.finish_file(target_file, tile)
        else:
            self.extract(tile)

    def _on_future_done(self, result: Future):
        with self.lock_running_futures:
            self.running_futures -= 1

        # access result so that exceptions get properly logged
        result.result()

    def extract(self, source: mercantile.Tile):
        """
        Extract all sub-tiles from the source until the sub-tiles reach the target size or the maximum zoom level
        has been reached.
        """
        z = source.z + 1
        if z > self.max_zoom:
            print_error(f'Asked to split up {source} but zoom level has reached maximum of {self.max_zoom}')
            return

        for x in [source.x * 2, source.x * 2 + 1]:
            for y in [source.y * 2, source.y * 2 + 1]:
                tile = mercantile.Tile(x, y, z)

                future = self.executor.submit(self._generate_tile, tile)
                with self.lock_running_futures:
                    self.running_futures += 1
                future.add_done_callback(lambda result: self._on_future_done(result))

    def finish_file(self, file: Path, tile: mercantile.Tile):
        """
        Do finishing steps for the given file.
        This includes copying the file to the output directory or uploading it toe a tileserver-mapping server.

        :param file: File which should be processed
        :param tile: Tile object whose data this file contains
        """
        subprocess.run(['rsync', str(file.absolute()), str(self.out_dir)], check=True)

        if self.should_upload:
            # check if a server object already describes this tile
            existing_dumps = json.loads(requests.get(f'{self.upload_url}/api/v1/planet_dumps/').content)
            target_dumps = [i for i in existing_dumps if i['x'] == tile.x and i['y'] == tile.y and i['z'] == tile.z]

            if len(target_dumps) == 0:
                # if no corresponding dump objects exists on the server, we need to create one
                response = json.loads(requests.post(f'{self.upload_url}/api/v1/planet_dumps/', headers={
                    'Content-Type': 'application/json'
                }, data=json.dumps({
                    'x': tile.x,
                    'y': tile.y,
                    'z': tile.z,
                }), auth=HTTPBasicAuth(username=self.upload_auth[0], password=self.upload_auth[1])).content)
                dump_id = response['id']
            else:
                dump_id = target_dumps[0]['id']

            # update only the file of the existing dump object on the server
            subprocess.run([
                'curl',
                '-u', f'{self.upload_auth[0]}:{self.upload_auth[1]}',
                '-F', f'file=@{file.absolute()}',
                '--request', 'PATCH',
                '--silent',
                f'{self.upload_url}/api/v1/planet_dumps/{dump_id}/'
            ], check=True, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    p = Program()
    p.run()
