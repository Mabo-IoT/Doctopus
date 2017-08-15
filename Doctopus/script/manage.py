# -*- coding: utf-8 -*-

import waitress
import argparse
from Doctopus.web.app import get_app

if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='A distributed data collector.')
    parse.add_argument('action', action='store')
    command = parse.parse_args().action
    if command == 'run':
        waitress.serve(get_app(), host='0.0.0.0', port=8000)
