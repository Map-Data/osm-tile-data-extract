#! /usr/bin/env python3
import os
import argparse
import subprocess
import time

import mercantile
from multiprocessing import Lock
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path


class Program:
    @staticmethod
    def parse_args() -> argparse.Namespace:
        def directory_type(raw: str):
            p = Path(raw)
            p.mkdir(exist_ok=True)
            if not p.is_dir():
                raise argparse.ArgumentTypeError(f'Path {raw} is not a directory')
            return p

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
                            help='Target size which the split files should have')
        return parser.parse_args()

    def __init__(self):
        args = self.parse_args()
        self.working_dir = args.working_dir
        self.out_dir = args.output_dir
        self.target_size = args.target_size
        self.planet_dump_url = args.planet_dump

        self.running_futures = 0
        self.lock_running_futures = Lock()

        self.executor = ThreadPoolExecutor(max_workers=(max(1, os.cpu_count() - 2)))

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

        parent_file = self.working_dir / f'{parent[2]}_{parent[0]}_{parent[1]}.pbf'
        target_file = self.working_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'

        if not parent_file.exists():
            print(f'Not generating {tile} because parent does not exist')
            return

        if parent_file.stat().st_size < self.target_size:
            print(f'Not generating {tile} because parent has reached target size')
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
            print(f'{tile} has reached target size')
            os.rename(str(target_file), str(self.out_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'))
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
        if z >= 10:
            print(f'Asked to extract tiles from {source} but zoom level has reached maximum')
            return

        for x in range(source.x, source.x + 2):
            for y in range(source.y, source.y + 2):
                tile = mercantile.Tile(x, y, z)

                future = self.executor.submit(self._generate_tile, tile)
                with self.lock_running_futures:
                    self.running_futures += 1
                future.add_done_callback(lambda result: self._on_future_done(result))


if __name__ == '__main__':
    p = Program()
    p.run()
