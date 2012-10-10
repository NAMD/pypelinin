#!/usr/bin/env python2
# coding: utf-8

from sys import stdout
from logging import Logger, StreamHandler, Formatter
from pypelinin import Router


def main():
    logger = Logger('Test Router')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    api_host_port = ('*', 5555)
    broadcast_host_port = ('*', 5556)
    default_config = {'store': {'data': 'test'}, 'monitoring interval': 60, }
    router = Router(api_host_port, broadcast_host_port, default_config, logger)
    router.start()

if __name__ == '__main__':
    main()
