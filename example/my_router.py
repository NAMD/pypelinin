#!/usr/bin/env python2
# coding: utf-8

from sys import stdout
from logging import Logger, StreamHandler, Formatter
from pypelinin import Router


def main():
    logger = Logger('My Router')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    api_host_port = ('*', 5555)
    broadcast_host_port = ('*', 5556)
    default_config = {'store': {'monitoring filename': '/tmp/monitoring.log'},
                      'monitoring interval': 60, }
    router = Router(api_host_port, broadcast_host_port, default_config, logger)
    router.start()

if __name__ == '__main__':
    main()
