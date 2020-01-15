#! /usr/bin/env python3

import os
from pathlib import Path

import mercantile

def process_asyc(command):
    com2 = "srun {}"
    os.system(com2)

path = "extracts"
tiles = []
for i in range(1, 10):
    for x in range(0, (2**i)):
        threads = []
        for y in range(0, (2**i)):
            tile = [x, y, i]
            box = mercantile.bounds(tile)
            parent = mercantile.parent(tile)
            parent_file = Path("{}/{}_{}_{}.pbf".format(
                path,
                parent[2],
                parent[0],
                parent[1]))
            tile_path = Path("{}/{}_{}_{}.pbf".format (
                path,
                tile[2],
                tile[0],
                tile[1]))
            if not parent_file.exists():
                # parent was not build
                continue
            if parent_file.stat().st_size < 1.5*10**9:
                print("Do not build {} because parent is small".format(tile))
                if parent_file.as_posix() not in tiles:
                    tiles.append(parent_file.as_posix())
                continue
            if tile_path.exists() and parent_file.stat().st_mtime < tile_path.stat().st_mtime:
                print("Do not build {} because parent is older".format(tile))
                continue
            com = "osmconvert -b={},{},{},{} -o={} --complete-ways --complex-ways {}".format(
                box.west,
                box.south,
                box.east,
                box.north,
                tile_path.as_posix(),
                parent_file.as_posix())
            print(com)
            #t = Thread(target = process_async, args = (com, ))
            #threads.append(t)
            os.system(com)
        for thread in threads:
            thread.join()
print(tiles)
