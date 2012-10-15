# coding: utf-8

from itertools import product
from time import time

from pygraph.classes.digraph import digraph as DiGraph
from pygraph.algorithms.cycles import find_cycle
from pygraph.readwrite.dot import write

from . import Client


class Worker(object):
    pass

class Job(object):
    def __init__(self, worker_name, data=None):
        self.worker_name = worker_name
        self.data = data

    def __repr__(self):
       #TODO: change this when add `input`
        return 'Job({})'.format(repr(self.worker_name))

    def __eq__(self, other):
        #TODO: change this when add `input`
        return type(self) == type(other) and \
               self.worker_name == other.worker_name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        #TODO: change this when add `input`
        return hash(self.worker_name) #TODO: change this when add `input`

    def serialize(self):
        #TODO: change this when add `input`
        if self.data is not None:
            return tuple({'worker_name': self.worker_name,
                          'data': tuple(self.data.items())}.items())
        else:
            return tuple({'worker_name': self.worker_name}.items())

    @staticmethod
    def deserialize(information):
        information = dict(information)
        if 'worker_name' not in information:
            raise ValueError('`worker_name` was not specified')
        elif 'data' not in information:
            return Job(information['worker_name'])
        else:
            return Job(information['worker_name'], dict(information['data']))


class Pipeline(object):
    def __init__(self, pipeline, data=None):
        #TODO: should raise if pipeline is not composed of `Job`s?
        self.data = data
        self.id = None
        self._finished_jobs = set()
        self._original_graph = self._graph = pipeline
        if type(pipeline) == dict:
            self._normalize()
        self._check_types()
        self._define_jobs()
        self._define_starters()
        self._create_digraph()
        if not self._validate():
            raise ValueError('The pipeline graph have cycles or do not have a '
                             'starter job')
        if data is not None:
            for job in self.jobs:
                job.data = data
                job.pipeline = self

        self._dependencies = {job: set() for job in self.jobs}
        for job_1, job_2 in self._graph:
            if job_2 is None:
                continue
            self._dependencies[job_2].add(job_1)
        self.sent_jobs = set()

    def __eq__(self, other):
        return self._graph == other._graph and self.data == other.data

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.serialize())

    def _normalize(self):
        new_graph = []
        for keys, values in self._original_graph.items():
            if type(keys) is Job:
                keys = [keys]
            if type(values) not in (tuple, list):
                values = [values]
            for key in keys:
                if not values:
                    new_graph.append((key, None))
                else:
                    for value in values:
                        new_graph.append((key, value))
        self._graph = new_graph

    def _check_types(self):
        for key, value in self._graph:
            if type(key) is not Job or type(value) not in (Job, type(None)):
                raise ValueError('Only `Job` objects are accepted')

    def _define_jobs(self):
        nodes = set()
        for key, value in self._graph:
            nodes.add(key)
            nodes.add(value)
        nodes.discard(None)
        self.jobs = tuple(nodes)

    def _define_starters(self):
        possible_starters = set()
        others = set()
        for key, value in self._graph:
            others.add(value)
            possible_starters.add(key)
        self.starters = tuple(possible_starters - others)

    def _create_digraph(self):
        digraph = DiGraph()
        digraph.add_nodes(self.jobs)
        for edge in self._graph:
            if edge[1] is not None:
                digraph.add_edge(edge)
        self._digraph = digraph

    def _validate(self):
        #TODO: test A -> B, A -> C, B -> C
        if len(self.starters) == 0:
            return False
        if find_cycle(self._digraph):
            return False
        return True

    def to_dot(self):
        return write(self._digraph)

    def serialize(self):
        result = []
        for key, value in self._graph:
            serialized_key = key.serialize()
            serialized_value = None
            if value is not None:
                serialized_value = value.serialize()
            result.append((serialized_key, serialized_value))
        data = self.data
        if data is not None:
            data = tuple(self.data.items())
        return tuple({'graph': tuple(result), 'data': data}.items())

    @staticmethod
    def _deserialize(info):
        info = dict(info)
        new_graph = []
        for key, value in info['graph']:
            deserialized_key = Job.deserialize(key)
            deserialized_value = None
            if value is not None:
                deserialized_value = Job.deserialize(value)
            new_graph.append((deserialized_key, deserialized_value))
        data = info['data']
        if data is not None:
            data = dict(data)
        return new_graph, data

    @staticmethod
    def deserialize(info):
        new_graph, data = Pipeline._deserialize(info)
        return Pipeline(new_graph, data=data)

class PipelineForPipeliner(Pipeline):
    @staticmethod
    def deserialize(info):
        new_graph, data = PipelineForPipeliner._deserialize(info)
        return PipelineForPipeliner(new_graph, data=data)

    def add_finished_job(self, job):
        if job not in self.jobs:
            raise ValueError('Job {} not in pipeline'.format(job))
        elif job in self._finished_jobs:
            raise RuntimeError('Job {} was already declared as '
                               'finished'.format(job))
        self._finished_jobs.add(job)

    def finished_job(self, job):
        return job in self._finished_jobs

    def finished(self):
        return set(self.jobs) == self._finished_jobs

    def available_jobs(self):
        available = set()
        for job in self.jobs:
            if self._dependencies[job].issubset(self._finished_jobs) and \
               job not in self._finished_jobs:
                available.add(job)
        return available

class PipelineManager(Client):
    #TODO: is there any way to subscribe to job ids? So I can know the status
    #      of each job in pipeline with this object
    def __init__(self, api, broadcast, poll_time=50): # milliseconds
        super(PipelineManager, self).__init__()
        self.poll_time = poll_time
        self._pipelines = []
        self._pipeline_from_id = {}
        self.connect(api=api, broadcast=broadcast)

    def start(self, pipeline):
        if pipeline in self._pipelines:
            raise ValueError('This pipeline was already started')
        self._pipelines.append(pipeline)
        request = {'command': 'add pipeline', 'pipeline': pipeline.serialize()}
        self.send_api_request(request)
        result = self.get_api_reply()
        pipeline_id = str(result['pipeline id'])
        pipeline.id = pipeline_id
        pipeline.finished = False
        self._pipeline_from_id[pipeline_id] = pipeline
        pipeline.started_at = time()
        self.broadcast_subscribe('pipeline finished: id=' + pipeline_id)
        return pipeline_id

    def _update_broadcast(self):
        while self.broadcast_poll(self.poll_time):
            message = self.broadcast_receive()
            if message.startswith('pipeline finished: '):
                try:
                    data = message.split(': ')[1].split(', ')
                    pipeline_id = data[0].split('=')[1]
                    duration = float(data[1].split('=')[1])
                except (IndexError, ValueError):
                    continue
                pipeline = self._pipeline_from_id[pipeline_id]
                pipeline.duration = duration
                pipeline.finished = True
                self.broadcast_unsubscribe(message)

    def finished(self, pipeline):
        if pipeline not in self._pipelines:
            raise ValueError('This pipeline is not being managed by this '
                             'PipelineMager')
        self._update_broadcast()
        return pipeline.finished
