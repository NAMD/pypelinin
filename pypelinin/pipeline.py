#!/usr/bin/env python
# coding: utf-8

#TODO: in future, pipeliner could be a worker in a broker tagged as pipeliner,
#      but router needs to support broker tags

from uuid import uuid4
import json
from . import Client


def _to_dict(obj, classkey=None):
    if isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = _to_dict(obj[k], classkey)
        return obj
    elif hasattr(obj, "__iter__"):
        return [_to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, _to_dict(value, classkey))
            for key, value in obj.__dict__.iteritems()
            if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj

class Worker(object):
    def __init__(self, worker_name):
        self.name = worker_name
        self.after = []

    def then(self, *after):
        self.after.extend(list(after))
        return self

    def __or__(self, after):
        self.then(*[after])
        return self

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return "Worker({name})".format(**self.__dict__)

    def serialize(self):
        if not self.after:
            return "worker: {name}".format(name=self.name)
        else:
            data = "main: worker: {name}".format(name=self.name)
            for node in self.after:
                data += " " + node.serialize()
            return data

    @staticmethod
    def from_json(value):
        temp_after = []
        data = json.loads(value)

        if isinstance(data, list):
            for node in data:
                temp_after.append(Worker.from_json(json.dumps(node)))
            return temp_after

        worker = Worker(data['name'])
        worker.after = data['after']
        for node in worker.after:
            temp_after.append(Worker.from_json(json.dumps(node)))

        worker.after = temp_after
        return worker

    def to_dict(self, classkey=None):
        return _to_dict(self, classkey)

class Pipeliner(Client):
    #TODO: should send monitoring information?
    #TODO: should receive and handle a 'job error' from router when some job
    #      could not be processed (timeout, worker not found etc.)

    def __init__(self, api_host_port, broadcast_host_port, logger=None,
                 poll_time=50):
        super(Pipeliner, self).__init__()
        self.api_host_port = api_host_port
        self.broadcast_host_port = broadcast_host_port
        self.logger = logger
        self.poll_time = poll_time
        self._new_pipelines = 0
        self._messages = []
        self._pipelines = {}
        self._jobs = {}
        self.logger.info('Pipeliner started')

    def start(self):
        try:
            self.connect(self.api_host_port, self.broadcast_host_port)
            self.broadcast_subscribe('new pipeline')
            self.run()
        except KeyboardInterrupt:
            self.logger.info('Got SIGNINT (KeyboardInterrupt), exiting.')
            self.close_sockets()

    def _update_broadcast(self):
        if self.broadcast_poll(self.poll_time):
            message = self.broadcast_receive()
            self.logger.info('Received from broadcast: {}'.format(message))
            if message.startswith('new pipeline'):
                self._new_pipelines += 1
            else:
                self._messages.append(message)

    def router_has_new_pipeline(self):
        self._update_broadcast()
        return self._new_pipelines > 0

    def ask_for_a_pipeline(self):
        self.send_api_request({'command': 'get pipeline'})
        message = self.get_api_reply()
        #TODO: if router stops and doesn't answer, pipeliner will stop here
        if 'data' in message:
            if message['data'] is not None:
                self.logger.info('Got this pipeline: {}'.format(message))
                self._new_pipelines -= 1
                return message
        elif 'pipeline' in message and message['pipeline'] is None:
            self.logger.info('Bad bad router, no pipeline for me.')
            return None
        else:
            self.logger.info('Ignoring malformed pipeline: {}'.format(message))
            #TODO: send a 'rejecting pipeline' request to router
            return None

    def get_a_pipeline(self):
        data = self.ask_for_a_pipeline()
        if data is not None:
            self.start_pipeline(data)

    def _send_job(self, worker):
        job = {'command': 'add job', 'worker': worker.name,
               'data': worker.data}
        self.logger.info('Sending new job: {}'.format(job))
        self.send_api_request(job)
        self.logger.info('Sent job: {}'.format(job))
        message = self.get_api_reply()
        self.logger.info('Received from router API: {}'.format(message))
        self._jobs[message['job id']] = worker
        subscribe_message = 'job finished: {}'.format(message['job id'])
        self.broadcast_subscribe(subscribe_message)
        self.logger.info('Subscribed on router Broadcast to: {}'\
                         .format(subscribe_message))

    def start_pipeline(self, data):
        pipeline_id = data['pipeline id']
        workers = Worker('downloader')
        workers.pipeline = pipeline_id
        workers.data = data['data']
        self._pipelines[pipeline_id] = [workers]
        self._send_job(workers)

    def verify_jobs(self):
        self._update_broadcast()
        new_messages = []
        for message in self._messages:
            if message.startswith('job finished: '):
                job_id = message.split(': ')[1].split(' ')[0]
                self.logger.info('Processing finished job id {}.'.format(job_id))
                worker = self._jobs[job_id]
                self._pipelines[worker.pipeline].remove(worker)
                for next_worker in worker.after:
                    next_worker.data = worker.data
                    next_worker.pipeline = worker.pipeline
                    self._pipelines[worker.pipeline].append(next_worker)
                    self._send_job(next_worker)
                del self._jobs[job_id]
                if not self._pipelines[worker.pipeline]:
                    self.send_api_request({'command': 'pipeline finished',
                                           'pipeline id': worker.pipeline})
                    self.get_api_reply()
                    #TODO: check reply
                    del self._pipelines[worker.pipeline]
                    self.logger.info('Finished pipeline {}'\
                                     .format(worker.pipeline))
                    self.get_a_pipeline()
                self.broadcast_unsubscribe(message)
        self._messages = []

    def run(self):
        self.logger.info('Entering main loop')
        self.get_a_pipeline()
        while True:
            if self.router_has_new_pipeline():
                self.get_a_pipeline()
            self.verify_jobs()
