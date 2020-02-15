#! /usr/bin/env python3
import concurrent
import json
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import List

import mercantile

# Todo: Make configurable
path = "extracts"
executor = ThreadPoolExecutor(max_workers=8)
generated_tiles_file = Path('generated_tiles.json')
start_zoom = 0
end_zoom = 10

smallest_tiles = []  # here we save the smallest generated tiles for each region
all_generated_tiles = []
futures = {}
running_futures = 0


def process_asyc(tile: List[int], parent_tile: List[int]):
    tile_path = get_tile_path(tile)
    parent_path = get_tile_path(parent_tile)
    box = mercantile.bounds(tile)
    command = "osmconvert -b={},{},{},{} -o={} --complete-ways --complex-ways {}".format(
        box.west,
        box.south,
        box.east,
        box.north,
        tile_path.as_posix(),
        parent_path.as_posix())
    print("Start Processing: {}".format(tile))
    start = time.time()
    os.system(command)
    print("Finish Processing: {} in {:.0f} seconds".format(tile, time.time() - start))
    enqueue_children(tile)


def get_tile_path(tile: List[int]) -> Path:
    return Path("{}/{}_{}_{}.pbf".format(path, tile[2], tile[0], tile[1]))


def future_finished(future):
    global running_futures
    running_futures -= 1


def enqueue_children(tile):
    if tile[2] >= end_zoom:
        return
    tile_path = get_tile_path(tile)
    if tile_path.stat().st_size < 1.5 * 10 ** 9:
        print("Do not build children of {} because it is small".format(tile))
        smallest_tiles.append(tile_path.as_posix())
        return
    children = mercantile.children(tile)
    for child in children:
        child_path = get_tile_path(child)
        if child_path.exists() and tile_path.stat().st_mtime < child_path.stat().st_mtime:
            print("Do not build {} because parent is older".format(child))
        else:
            print("Enqueue {}".format(child))
            future = executor.submit(process_asyc, child, tile)
            global running_futures
            running_futures += 1
            future.add_done_callback(future_finished)
            futures[future] = child


def enqueue_initial():
    for x in range(0, (2 ** start_zoom)):
        for y in range(0, (2 ** start_zoom)):
            tile = [x, y, start_zoom]
            if get_tile_path(tile).exists():
                enqueue_children(tile)


def check_finished():
    for future in concurrent.futures.as_completed(futures.copy()):
        orig_tile_name = futures[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (orig_tile_name, exc))
        else:
            # calculation ok
            pass


if __name__ == '__main__':
    enqueue_initial()
    try:
        while running_futures:
            print("Queue length: {}".format(running_futures))
            time.sleep(600)
            check_finished()
    except KeyboardInterrupt:
        executor.shutdown()
        raise
    executor.shutdown()
    print("Build {} tiles, {} in the smalest set".format(len(all_generated_tiles), len(smallest_tiles)))
    with open(generated_tiles_file, 'w') as fp:
        json.dump({'all': all_generated_tiles, 'smallest': smallest_tiles}, fp)
    print("Finished")
