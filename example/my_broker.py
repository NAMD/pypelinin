#!/usr/bin/env python
# coding: utf-8

import json
from sys import stdout
from logging import Logger, StreamHandler, Formatter, NullHandler
from pypelinin import Broker


class MyStore(object):
    def __init__(self, **configuration):
        self.monitoring = open('/tmp/broker-monitoring', 'w')

    def retrieve(self, data):
        #data = {'worker_name': ..., 'data': ..., 'worker_meta': ...}
        return data['data']

    def save(self, data):
        #data = {'worker_name': ..., 'worker_result': ..., 'worker_meta': ...}
        pass

    def save_monitoring(self, data):
        data_as_json_string = json.dumps(data)
        self.monitoring.write(data_as_json_string + "\n")
        self.monitoring.flush()

def main():
    logger = Logger('Broker')
    handler = StreamHandler(stdout)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                          '%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    broker = Broker(api='tcp://localhost:5555',
                    broadcast='tcp://localhost:5556', store_class=MyStore,
                    logger=logger, workers='workers')
    broker.start()

if __name__ == '__main__':
    main()
