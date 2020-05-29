#!/usr/bin/env python3

import argparse
import os

from pathlib import Path
from osm_tile_data_extract.generate_extracts import Program as GenerateExtracts
from osm_tile_data_extract.import_extracts import Program as ImportExtracts


def parse_args() -> argparse.Namespace:
    def directory_type(raw: str):
        p = Path(raw)
        p.mkdir(exist_ok=True)
        if not p.is_dir():
            raise argparse.ArgumentTypeError(f'Path {raw} is not a directory')
        return p

    def auth_type(raw: str) -> list:
        if ':' not in raw or len(raw.split(':')) != 2:
            raise argparse.ArgumentTypeError('Authentication has invalid format')
        return raw.split(':')

    parser = argparse.ArgumentParser('osm-tile-data-extract')
    parser.add_argument('-w', '--working-dir', dest='working_dir', type=directory_type,
                        default=os.path.join(os.path.dirname(__file__), 'tmp'),
                        help='Working directory in which intermediate and temporary files are stored')
    parser.add_argument('-o', '--output-dir', dest='output_dir', type=directory_type,
                        default=os.path.join(os.path.dirname(__file__), 'out'))
    parser.add_argument('--mapping-url', dest='mapping_url', type=str, required=True,
                        help='Base URL under which a tileserver-mapping server is reachable')
    parser.add_argument('--mapping-auth', dest='mapping_auth', type=auth_type, required=True,
                        help='<username>:<password> combination used to authenticate at the tileserver-mapping')

    sub_parsers = parser.add_subparsers(dest='command')
    GenerateExtracts.add_args(sub_parsers.add_parser('generate-extracts',
                                                     description='Extract similarly sized files from  the latest'
                                                                 'OpenStreetMap planet dump'))
    ImportExtracts.add_args(sub_parsers.add_parser('import-extracts',
                                                   description='Import one pbf extract into postgresql and afterwards'
                                                               'dump the database'))

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.command == 'generate-extracts':
        program = GenerateExtracts(args)
    elif args.command == 'import-extracts':
        program = ImportExtracts(args)

    program.run()
