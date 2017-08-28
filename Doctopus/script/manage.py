# -*- coding: utf-8 -*-

import waitress
import argparse
from Doctopus.web.app import get_app

try:
    from queue import Queue
except:
    from Queue import Queue

from logging import getLogger
from threading import Thread
from plugins.your_plugin import *

from Doctopus.lib.Sender import Sender
from Doctopus.lib.watchdog import WatchDog
from Doctopus.lib.communication import Communication
from Doctopus.lib.logging_init import setup_logging
from Doctopus.utils.util import get_conf

log = getLogger("start")

def start():
    # init queues
    queue = {'data_queue': Queue(), 'sender': Queue() }

    # load all configs
    all_conf = get_conf('conf/conf.toml')

    # init log config
    setup_logging(all_conf['log_configuration'])

    # init instances
    checker = MyCheck(all_conf)
    handler = MyHandler(all_conf)
    sender = Sender(all_conf)
    communication = Communication(all_conf)

    # name instances
    checker.name = 'checker'
    handler.name = 'handler'
    sender.name = 'sender'
    communication.name = 'communication'

    # init work threads set
    workers = [checker, handler]

    thread_set = dict()

    # start workers instance
    for worker in workers:
        thread = Thread(target=worker.work, args=(queue,), name='%s' % worker.name)
                        #kwargs={'name': worker.name},
                        #name='%s ' % worker.name)
        thread.setDaemon(True)
        thread.start()
        thread_set[worker.name] = thread

    # init send set
    send_set = [communication, sender]
    for send in send_set:
        thread = Thread(target=send.work, args=(queue,), name='%s' % send.name)
        thread.setDaemon(True)
        thread.start()

    # start watch instance
    watch = WatchDog(all_conf)
    watch = Thread(target=watch.work, name='watchdog', args=(thread_set, queue, workers))
    watch.setDaemon(True)
    watch.start()

if __name__ == '__main__':
    parse = argparse.ArgumentParser(prog='Doctopus', description='A distributed data collector.')
    parse.add_argument('-a', '--action', action='store', default='run', help='Run/test the project, default run')
    parse.add_argument('-v', '--version', action='version', default=None, version='%(prog)s 0.1.0')
    parse.add_argument('-t', '--target', default='client', choices=['server', 'client'],
                       help='selelct the target, default client')
    parse.add_argument('-i', '--ip', default='0.0.0.0', help="Hostname or IP address on which to listen, default is '0.0.0.0', "
                                                               "which means 'all IP addresses on this host'.")
    parse.add_argument('-p', '--port', default='8000', help="TCP port on which to listen, default is '8000'.")

    command = parse.parse_args().action
    target = parse.parse_args().target
    host = parse.parse_args().ip
    port = parse.parse_args().port
    version = parse.parse_args().version

    if command == 'run':
        start()
        waitress.serve(get_app(target), host=host, port=port, _quiet=True)
    elif command == 'test':
        pass
    elif version:
        print(version)
