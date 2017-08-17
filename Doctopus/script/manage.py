# -*- coding: utf-8 -*-

import waitress
import argparse
from Doctopus.web.app import get_app

if __name__ == '__main__':
    parse = argparse.ArgumentParser(prog='Doctopus', description='A distributed data collector.')
    parse.add_argument('-a', '--action', action='store', default='run', help='Run/test the project, default run')
    parse.add_argument('-v', '--version', action='version', default=None, version='%(prog)s 0.1.0')
    parse.add_argument('-t', '--target', default='client', choices=['server', 'client'],
                       help='selelct the target, default client')
    command = parse.parse_args().action
    target = parse.parse_args().target
    version = parse.parse_args().version
    if command == 'run':
        waitress.serve(get_app(target), host='0.0.0.0', port=8000)
    elif command == 'test':
        pass
    elif version:
        print(version)
