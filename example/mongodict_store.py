# coding: utf-8

from pymongo import Connection
from mongodict import MongoDict


#TOOD: needs testing
class MongoDictStore(object):
    '''Sample Store based on MongoDict'''

    def __init__(self, **configuration):
        '''Instantiate a MongoDictStore

        `configuration` must have the keys:
        - host
        - port
        - database
        - collection (for MongoDict)
        - monitoring_collection
        '''
        self._dict = MongoDict(**configuration, safe=True)
        self._connection = Connection(configuration['host'],
                                      configuration['port'], safe=True)
        self._db = self._connection[configuration['database']]
        self._monitoring = self._db[configuration['monitoring_collection']]

    def retrieve(self, info):
        '''Retrieve data to pass to `WorkerClass.process`

        `info` has keys 'worker', 'worker_requires' and 'data':
            - 'data' comes from pipeline data
            - 'worker' is the worker name
            - 'worker_requires' is 'requires' attribute of WorkerClass

        For MongoDictStore, 'data' must have an 'id' key
        '''
        data_id = info['data']['id']
        worker_input = {}
        for key in info['worker_requires']:
            mapped_key = 'id:{}:{}'.format(data_id, key)
            worker_input[key] = self._dict.get(mapped_key, None)
        return worker_input

    def save(self, info):
        '''Save information returned by `WorkerClass.process`

        `info` has keys 'worker', 'worker_requires', 'worker_result' and 'data':
            - 'data' comes from pipeline data
            - 'worker' is the worker name
            - 'worker_requires' is 'requires' attribute of WorkerClass
            - 'worker_result' is what WorkerClass.process returned
        '''
        data_id = info['data']['id']
        for key, value in info['worker_result'].items():
            mapped_key = 'id:{}:{}'.format(data_id, key)
            list_key = 'id:{}:_keys'.format(data_id)
            self._dict[mapped_key] = value
            if list_key not in self._dict:
                self._dict[list_key] = [key]
            else:
                data = self._dict[list_key]
                data.append(key)
                self._dict[list_key] = data

    def save_monitoring(self, data):
        self._monitoring.insert(data)
