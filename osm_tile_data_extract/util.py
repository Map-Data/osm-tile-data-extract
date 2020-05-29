import sys
import mercantile
import subprocess
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
    def __init__(self, mapping_url: str, username: str, password: str):
        self.username = username
        self.password = password

        self.app = App._create_(f'{mapping_url}/schema.json')
        auth = Security(self.app)
        auth.update_with('Basic', (username, password))
        self.client = Client(auth)

    def get_planet_dump(self, tile: mercantile.Tile) -> dict:
        operation = self.app.op['api_v1_planet_dumps_list']
        response = self.client.request(operation()).data
        matching_dumps = [i for i in response if i['x'] == tile.x and i['y'] == tile.y and i['z'] == tile.z]

        if len(matching_dumps) == 0:
            raise LookupError(f'Planet dump for {tile} does not exist')
        else:
            return matching_dumps[0]

    def _upload_dump(self, dump_type: str, tile: mercantile.Tile, dump_path: str):
        # check if a dump object already exists
        operation = self.app.op[f'api_v1_{dump_type}_list']
        response = self.client.request(operation()).data
        matching_dumps = [i for i in response if i['x'] == tile.x and i['y'] == tile.y and i['z'] == tile.z]

        if len(matching_dumps) == 0:
            # create new object
            operation = self.app.op[f'api_v1_{dump_type}_create']
            subprocess.run(['curl', '-u', f'{self.username}:{self.password}',
                            '-F', f'file=@{dump_path}', '-F', f'x={tile.x}', '-F', f'y={tile.y}', '-F', f'z={tile.z}',
                            '--request', str(operation.method).upper(),
                            f'{self.app.schemes[0]}://{self.app.root.host}{operation.path}'],
                           check=True, stdout=subprocess.DEVNULL)

        else:
            # patch existing object
            dump_id = matching_dumps[0]['id']
            operation = self.app.op[f'api_v1_{dump_type}_partial_update']
            subprocess.run(['curl', '-u', f'{self.username}:{self.password}', '-F', f'file=@{dump_path}',
                            '--request', str(operation.method).upper(),
                            str(f'{self.app.schemes[0]}://{self.app.root.host}{operation.path}').format(id=dump_id)],
                           check=True, stdout=subprocess.DEVNULL)

    def upload_planet_dump(self, tile: mercantile.Tile, dump_path: str):
        self._upload_dump('planet_dumps', tile, dump_path)

    def upload_sql_dump(self, tile: mercantile.Tile, dump_path: str):
        self._upload_dump('postgresql_dumps', tile, dump_path)
