#! /usr/bin/env python3
import os
import argparse
import subprocess
import mercantile
from pathlib import Path


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
    print(f"Downloading {url}")
    subprocess.run(['wget', '-N', '-nv', '--show-progress', url], check=True, cwd=str(working_dir))
    file_name = url.rsplit('/', 1)[-1]
    (working_dir / file_name).link_to(str(working_dir / 'source.osm.pbf'))


def process_asyc(command):
    com2 = "srun {}"
    os.system(com2)


def extract(working_dir: Path, out_dir: Path, target_size: int):
    (working_dir / 'source.osm.pbf').link_to(str(working_dir / '0_0_0.pbf'))
    tiles = []

    for z in range(1, 10):
        for x in range(0, (2**z)):
            for y in range(0, (2**z)):
                tile = mercantile.Tile(x, y, z)
                box = mercantile.bounds(tile)
                parent = mercantile.parent(tile)

                parent_file = working_dir / f'{parent[2]}_{parent[0]}_{parent[1]}.pbf'
                target_file = working_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'

                if not parent_file.exists() or parent_file.stat().st_size < target_size:
                    # parent was not build or has reached target size
                    continue

                if not target_file.exists() or parent_file.stat().st_mtime > target_file.stat().st_mtime:
                    # only build file if it does not exist
                    # or parent file has been modified since target was last generated
                    cmd = [
                        'osmconvert',
                        f'-b={box.west},{box.south},{box.east},{box.north}',
                        f'-o={target_file}',
                        '--complete-ways',
                        '--complex-ways',
                        '--out-pbf',
                        str(parent_file)
                    ]
                    subprocess.run(cmd, cwd=str(working_dir))

                if target_file.stat().st_size < target_size:
                    print(f'{tile} has reached target size')
                    tiles.append(str(target_file))
                    os.rename(str(target_file), str(out_dir / f'{tile[2]}_{tile[0]}_{tile[1]}.pbf'))

    print(tiles)


if __name__ == '__main__':
    args = parse_args()
    download_planet_dump(args.planet_dump, args.working_dir)
    extract(args.working_dir, args.output_dir, args.target_size)
