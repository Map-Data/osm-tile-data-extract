#! /usr/bin/env python3
import os
import argparse
import subprocess
import mercantile
import tqdm
from multiprocessing.pool import Pool, ThreadPool
from pathlib import Path
from tqdm import tqdm


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
    parser.add_argument('-s', '--target-size', dest='target_size', default=1.5*10**9, type=int,
                        help='Target size which the split files should have')
    return parser.parse_args()


def download_planet_dump(url: str, working_dir: Path):
    print(f'Downloading {url}')
    subprocess.run(['wget', '-N', '-nv', '--show-progress', url], check=True, cwd=str(working_dir))

    file_name = url.rsplit('/', 1)[-1]
    subprocess.run(['ln', '-sf', (working_dir / file_name).absolute(), (working_dir / 'source.osm.pbf').absolute()])


def _process_tile(working_dir: Path, out_dir: Path, target_size: int, tile: mercantile.Tile):
    box = mercantile.bounds(tile)
    parent = mercantile.parent(tile)

    parent_file = working_dir / f'{parent[2]}_{parent[0]}_{parent[1]}.pbf'
    target_file = working_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'

    if not parent_file.exists():
        print(f'not generating {tile} because parent does not exist')
        return

    if parent_file.stat().st_size < target_size:
        print(f'not generating {tile} because parent has reached target size')
        return

    if not target_file.exists() or parent_file.stat().st_mtime > target_file.stat().st_mtime:
        # only build file if it does not exist
        # or parent file has been modified since target was last generated
        print(f'generating {tile}')
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

    if target_file.stat().st_size < target_size:
        print(f'{tile} has reached target size')
        os.rename(str(target_file), str(out_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'))


def extract(working_dir: Path, out_dir: Path, target_size: int):
    print('Extracting tiles')

    subprocess.run(['ln', '-sf', (working_dir / 'source.osm.pbf').absolute(), (working_dir / '0_0_0.pbf').absolute()])

    with tqdm(total=sum([2**(2*i) for i in range(1, 10)]), disable=True) as pbar:
        for z in range(1, 10):
            with ThreadPool(processes=max(os.cpu_count() - 2, 1)) as pool:
                for x in range(0, (2**z)):
                    for y in range(0, (2**z)):
                        pbar.update(1)

                        tile = mercantile.Tile(x, y, z)
                        pool.apply_async(_process_tile, [working_dir, out_dir, target_size, tile])
                pool.close()
                pool.join()


if __name__ == '__main__':
    args = parse_args()
    download_planet_dump(args.planet_dump, args.working_dir)
    extract(args.working_dir, args.output_dir, args.target_size)
