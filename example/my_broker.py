#!/usr/bin/env python
# coding: utf-8

from logging import Logger, StreamHandler, Formatter, NullHandler
from multiprocessing import cpu_count
from sys import stdout

from pypelinin import Broker

from file_store import SimpleFileStore


def main():
    logger = Logger('Broker')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    broker = Broker(api='tcp://localhost:5555',       # router API
                    broadcast='tcp://localhost:5556', # router Broadcast
                    # class that will be called to retrieve/store information
                    # to pass to/to save from worker
                    store_class=SimpleFileStore,
                    logger=logger,
                    # name of the module that contain workers
                    workers='workers',
                    #TODO: string or list of modules
                    # each core will run 4 workers
                    number_of_workers=cpu_count() * 4)
    broker.start()

if __name__ == '__main__':
    main()
