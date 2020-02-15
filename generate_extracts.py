#! /usr/bin/env python3
import concurrent
import json
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path

import mercantile


def process_asyc(command, tile):
    print("Start Processing: {}".format(tile))
    start = time.time()
    os.system(command)
    print("Finish Processing: {} in {:.0f} seconds".format(tile, time.time() - start))
    return 0


# Todo: Make configurable
path = "extracts"
executor = ThreadPoolExecutor(max_workers=2)
generated_tiles_file = Path('generated_tiles.json')

smallest_tiles = []  # here we save the smallest generated tiles for each region
all_generated_tiles = []

for i in range(1, 10):
    for x in range(0, (2 ** i)):
        futures = {}
        for y in range(0, (2 ** i)):
            tile = [x, y, i]
            tile_name = "{}/{}/{}".format(i, x, y)
            box = mercantile.bounds(tile)
            parent = mercantile.parent(tile)
            parent_file = Path("{}/{}_{}_{}.pbf".format(
                path,
                parent[2],
                parent[0],
                parent[1]))
            tile_path = Path("{}/{}_{}_{}.pbf".format(
                path,
                tile[2],
                tile[0],
                tile[1]))
            if not parent_file.exists():
                # parent was not build
                continue
            if parent_file.stat().st_size < 1.5 * 10 ** 9:
                print("Do not build {} because parent is small".format(tile_name))
                if parent_file.as_posix() not in smallest_tiles:
                    smallest_tiles.append(parent_file.as_posix())
                continue
            if tile_path.exists() and parent_file.stat().st_mtime < tile_path.stat().st_mtime:
                print("Do not build {} because parent is older".format(tile_name))
                continue
            all_generated_tiles.append(tile_path.as_posix())
            com = "osmconvert -b={},{},{},{} -o={} --complete-ways --complex-ways {}".format(
                box.west,
                box.south,
                box.east,
                box.north,
                tile_path.as_posix(),
                parent_file.as_posix())
            futures[executor.submit(process_asyc, com, tile)] = tile_name
        for future in concurrent.futures.as_completed(futures):
            orig_tile_name = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (orig_tile_name, exc))
            else:
                # calculation ok
                pass

print(smallest_tiles)
with open(generated_tiles_file, 'w') as fp:
    json.dump({'all': all_generated_tiles, 'smallest': smallest_tiles}, fp)
print("Finished")
