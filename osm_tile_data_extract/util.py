import sys
import mercantile
from urllib.parse import urlparse

from pyswagger import App, Security
from pyswagger.contrib.client.requests import Client


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_stage(m: str):
    print(f'{Colors.OKBLUE}-----> {m}{Colors.ENDC}')


def print_error(m: str):
    print(f'{Colors.FAIL}{m}{Colors.ENDC}', file=sys.stderr)


class ApiClient:
    def __init__(self, mapping_url: str, auth: list):
        self.app = App._create_(f'{mapping_url}/schema.json')
        auth = Security(self.app)
        self.client = Client(auth)

    def get_planet_dump(self, tile: mercantile.Tile) -> dict:
        operation = self.app.op['api_v1_planet_dumps_list']
        response = self.client.request(operation()).data
        matching_dumps = [i for i in response if i['x'] == tile.x and i['y'] == tile.y and i['z'] == tile.z]

        if len(matching_dumps) == 0:
            raise LookupError(f'Planet dump for {tile} does not exist')
        else:
            return matching_dumps[0]
