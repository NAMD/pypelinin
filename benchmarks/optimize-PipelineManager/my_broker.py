#!/usr/bin/env python
# coding: utf-8

from logging import Logger, StreamHandler, Formatter, NullHandler
from multiprocessing import cpu_count
from sys import stdout

from pypelinin import Broker


class NullStore(object):
    def __init__(self, *args, **kwargs):
        pass

    def retrieve(self, data):
        return {}

    def save(self, data):
        pass

    def save_monitoring(self, data):
        pass

def main():
    logger = Logger('Broker')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    broker = Broker(api='tcp://localhost:12345',       # router API
                    broadcast='tcp://localhost:12346', # router Broadcast
                    # class that will be called to retrieve/store information
                    # to pass to/to save from worker
                    store_class=NullStore,
                    logger=logger,
                    # name of the module that contain workers
                    workers='workers',
                    # each core will run 4 workers
                    number_of_workers=cpu_count() * 4)
    broker.start()

if __name__ == '__main__':
    main()
