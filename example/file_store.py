# coding: utf-8

import json


class SimpleFileStore(object):
    '''Sample Store based on files

    This store should NOT be used in production and will only works if you have
    only one Broker.
    '''
    def __init__(self, **configuration):
        self.monitoring_fp = open(configuration['monitoring filename'], 'w')

    def retrieve(self, info):
        '''Retrieve data to pass to `WorkerClass.process`

        `info` has keys 'worker', 'worker_requires' and 'data':
            - 'data' comes from pipeline data
            - 'worker' is the worker name
            - 'worker_requires' is 'requires' attribute of WorkerClass
        '''
        filename = info['data']['filename'] # get filename with data
        worker_requires = info['worker_requires']
        with open(filename, 'r') as fp:
            file_data = json.loads(fp.read().strip()) # read filename
        # get only information this worker needs
        worker_input = {key: file_data[key] for key in worker_requires}
        return worker_input

    def save(self, info):
        '''Save information returned by `WorkerClass.process`

        `info` has keys 'worker', 'worker_requires', 'worker_result' and 'data':
            - 'data' comes from pipeline data
            - 'worker' is the worker name
            - 'worker_requires' is 'requires' attribute of WorkerClass
            - 'worker_result' is what WorkerClass.process returned
        '''
        # read information from file
        filename = info['data']['filename']
        with open(filename, 'r') as fp:
            file_data = json.loads(fp.read().strip())

        # update file with information returned by worker
        worker_result = info['worker_result']
        file_data['_result-from-{}'.format(info['worker'])] = worker_result
        for key, value in worker_result.items():
            file_data[key] = value
        with open(filename, 'w') as fp:
            fp.write(json.dumps(file_data))

    def save_monitoring(self, data):
        # serialize monitoring information to JSON and save in a file
        data_as_json_string = json.dumps(data)
        self.monitoring_fp.write(data_as_json_string + "\n")
        self.monitoring_fp.flush()
