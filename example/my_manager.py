#!/usr/bin/env python2
# coding: utf-8

from sys import stdout
from logging import Logger, StreamHandler, Formatter
from pypelinin import Manager


def main():
    logger = Logger('Manager')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    api_host_port = ('*', 5555)
    broadcast_host_port = ('*', 5556)
    default_config = {'db': {'data': 'test'}, 'monitoring interval': 60, }
    manager = Manager(api_host_port, broadcast_host_port, default_config,
                      logger)
    manager.start()

if __name__ == '__main__':
    main()
