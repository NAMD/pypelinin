# coding: utf-8

#TODO: in future, pipeliner could be a worker in a broker tagged as pipeliner,
#      but router needs to support broker tags

from time import time
from uuid import uuid4
from . import Client, Job, PipelineForPipeliner


class Pipeliner(Client):
    #TODO: should send monitoring information?
    #TODO: should receive and handle a 'job error' from router when some job
    #      could not be processed (timeout, worker not found etc.)
    #TODO: max of pipelines per Pipeliner?
    #TODO: handle incorrect pipelines (ignored) - send message to Router

    def __init__(self, api, broadcast, logger=None, poll_time=50):
        super(Pipeliner, self).__init__()
        self._api_address = api
        self._broadcast_address = broadcast
        self.logger = logger
        self.poll_time = poll_time
        self._new_pipelines = None
        self._messages = []
        self._pipelines = {}
        self._jobs = {}
        self.logger.info('Pipeliner started')

    def start(self):
        try:
            self.connect(self._api_address, self._broadcast_address)
            self.broadcast_subscribe('new pipeline')
            self.run()
        except KeyboardInterrupt:
            self.logger.info('Got SIGNINT (KeyboardInterrupt), exiting.')
            self.disconnect()

    def _update_broadcast(self):
        if self.broadcast_poll(self.poll_time):
            message = self.broadcast_receive()
            self.logger.info('Received from broadcast: {}'.format(message))
            if message.startswith('new pipeline'):
                if self._new_pipelines is None:
                    self._new_pipelines = 1
                else:
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
        if 'pipeline' in message:
            if message['pipeline'] is not None:
                self.logger.info('Got this pipeline: {}'.format(message))
                if self._new_pipelines is None:
                    self._new_pipelines = 0
                else:
                    self._new_pipelines -= 1
                return message
            else:
                self._new_pipelines = 0
                self.logger.info('Bad bad router, no pipeline for me.')
                return None
        else:
            self.logger.info('Ignoring malformed pipeline: {}'.format(message))
            #TODO: send a 'rejecting pipeline' request to router
            return None

    def get_a_pipeline(self):
        pipeline_definition = 42
        while pipeline_definition is not None:
            pipeline_definition = self.ask_for_a_pipeline()
            if pipeline_definition is not None:
                pipeline = \
                        PipelineForPipeliner.deserialize(pipeline_definition['pipeline'])
                pipeline.id = pipeline_definition['pipeline id']
                pipeline.started_at = time()
                self._pipelines[pipeline.id] = pipeline
                self.start_pipeline_jobs(pipeline, pipeline.starters)

    def _start_job(self, job):
        job_request = {'command': 'add job', 'worker': job.worker_name,
                       'data': job.data}
        self.send_api_request(job_request)
        self.logger.info('Sent job request: {}'.format(job_request))
        message = self.get_api_reply()
        self.logger.info('Received from router API: {}'.format(message))
        job_id = message['job id']
        subscribe_message = 'job finished: {}'.format(job_id)
        self.broadcast_subscribe(subscribe_message)
        self.logger.info('Subscribed on router broadcast to: {}'\
                         .format(subscribe_message))
        return job_id

    def start_pipeline_jobs(self, pipeline, jobs):
        job_ids = []
        for job in jobs:
            job_id = self._start_job(job)
            self._jobs[job_id] = job
            pipeline.sent_jobs.add(job)

    def verify_jobs(self):
        self._update_broadcast()
        for message in self._messages:
            if message.startswith('job finished: '):
                job_id = message.split(': ')[1].split(' ')[0]
                if job_id in self._jobs:
                    self.logger.info('Processing finished job id {}.'.format(job_id))
                    job = self._jobs[job_id]
                    pipeline = job.pipeline
                    pipeline.add_finished_job(job)
                    del self._jobs[job_id]
                    if pipeline.finished():
                        total_time = time() - pipeline.started_at
                        self.send_api_request(
                                {'command': 'pipeline finished',
                                'pipeline id': pipeline.id,
                                'duration': total_time}
                        )
                        self.logger.info('Finished pipeline_id={}, '
                                         'duration={}'.format(pipeline.id,
                                                              total_time))
                        self.get_api_reply() #TODO: check reply
                        del self._pipelines[pipeline.id]
                        self.get_a_pipeline()
                    else:
                        jobs_to_send = pipeline.available_jobs() - pipeline.sent_jobs
                        if jobs_to_send:
                            self.start_pipeline_jobs(pipeline, jobs_to_send)
                self.broadcast_unsubscribe(message)
            elif message == 'new pipeline':
                self.get_a_pipeline()
        self._messages = []

    def run(self):
        self.logger.info('Entering main loop')
        self.get_a_pipeline()
        while True:
            if self.router_has_new_pipeline():
                self.get_a_pipeline()
            self.verify_jobs()
