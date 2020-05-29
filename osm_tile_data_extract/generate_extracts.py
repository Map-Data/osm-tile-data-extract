import os
import argparse
import subprocess
import time

import mercantile
from multiprocessing import Lock
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
from .util import *


class Program:
    @staticmethod
    def add_args(parser: argparse.ArgumentParser):
        parser.add_argument('-p', '--planet-dump', dest='planet_dump',
                            default='https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf',
                            help='Url of the source pbf file')
        parser.add_argument('-s', '--target-size', dest='target_size', default=1.5 * 10 ** 9, type=int,
                            help='Target files will not be larger than this size in bytes')
        parser.add_argument('-z', '--max-zoom', dest='max_zoom', default=9, type=int,
                            help='Maximum zoom level above which no further splitting will be performed')
        parser.add_argument('--processes', default=(max(1, os.cpu_count() - 2)), type=int,
                            help='How many concurrent processes to use')

    def __init__(self, args: argparse.Namespace):
        self.working_dir = args.working_dir
        self.out_dir = args.output_dir
        self.target_size = args.target_size
        self.max_zoom = args.max_zoom
        self.planet_dump_url = args.planet_dump

        self.running_futures = 0
        self.lock_running_futures = Lock()

        self.executor = ThreadPoolExecutor(max_workers=args.processes)
        self.api = ApiClient(args.mapping_url, args.mapping_auth[0], args.mapping_auth[1])

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
            self.api.upload_planet_dump(tile, str(target_file.absolute()))
            subprocess.run(['rsync', str(target_file.absolute()), str(self.out_dir)], check=True)
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
