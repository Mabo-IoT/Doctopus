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
    parse.add_argument('-h', '--host', default='0.0.0.0', help="Hostname or IP address on which to listen, default is '0.0.0.0', "
                                                               "which means 'all IP addresses on this host'.")
    parse.add_argument('-p', '--port', default='8000', help="TCP port on which to listen, default is '8000'.")

    command = parse.parse_args().action
    target = parse.parse_args().target
    host = parse.parse_args().host
    port = parse.parse_args().port
    version = parse.parse_args().version

    if command == 'run':
        waitress.serve(get_app(target), host=host, port=port)
    elif command == 'test':
        pass
    elif version:
        print(version)
