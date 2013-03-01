# coding: utf-8

from itertools import product
from textwrap import dedent
from time import time

from . import Client


class Worker(object):
    pass

class Job(object):
    def __init__(self, worker_name, data=None):
        self.worker_name = worker_name
        self.data = data

    def __repr__(self):
        #TODO: change this when add `input`
        data = ''
        if self.data is not None:
            data = ', data=...'
        return '<Job worker={}{}>'.format(self.worker_name, data)

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
        self.data = data
        self.id = None
        self._finished_jobs = set()
        self._original_graph = self._graph = pipeline
        if type(pipeline) == dict:
            self._normalize()
        self._check_types()
        self._define_jobs()
        self._define_starters()
        self._dependencies = {job: set() for job in self.jobs}
        for job_1, job_2 in self._graph:
            if job_2 is None:
                continue
            self._dependencies[job_2].add(job_1)
        self.sent_jobs = set()

        if not self._validate():
            raise ValueError('The pipeline graph have cycles or do not have a '
                             'starter job')
        if data is not None:
            for job in self.jobs:
                job.data = data
                job.pipeline = self

    def __eq__(self, other):
        return self._graph == other._graph and self.data == other.data

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.serialize())

    def __repr__(self):
        data = ''
        if self.data is not None:
            data = ', data=...'
        jobs = ', '.join([job.worker_name for job in self.jobs])
        return '<Pipeline: {}{}>'.format(jobs, data)

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

    def has_cycle(self):
        '''Verify if pipeline's graph has cycle

        Code extracted from:
            http://neopythonic.blogspot.com/2009/01/detecting-cycles-in-directed-graph.html
        '''
        ready = []
        todo = set(self.jobs)
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                # self._dependencies is a dict with key, value where each
                # key is a certain job and the value is the job related
                # to that job. In graph terms it represents all the
                # precedent jobs to the key job.
                # When dealing with cycle check, we can use them disregarding
                # the correct direction of the edge.
                for node in self._dependencies[top]:
                    if node in stack:
                        return stack[stack.index(node):]
                    if node in todo:
                        stack.append(node)
                        todo.remove(node)
                        break
                else:
                    node = stack.pop()
                    ready.append(node)
        return set(ready) < set(self.jobs)

    def _validate(self):
        return len(self.starters) != 0 and not self.has_cycle()

    def __unicode__(self):
        def represent(obj):
            if type(obj) is Job:
                return obj.worker_name
            else:
                return '(None)'
        nodes = u';\n    '.join([u'"{}"'.format(job.worker_name) \
                                 for job in self.jobs])
        edges = u';\n    '.join([u'"{}" -> "{}"'.format(represent(job1),
                                                        represent(job2)) \
                                 for job1, job2 in self._graph])
        return dedent(u'''
        digraph graphname {{
            {};

            {};
        }}''').strip().format(nodes, edges)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def save_dot(self, filename, encoding='utf-8'):
        '''Save a dot file `filename` (used by graphviz) with Pipeline data
        '''
        with open(filename, 'w') as fp:
            fp.write((unicode(self) + u'\n').encode(encoding))

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
        self._pipelines = {}
        self.started_pipelines = 0
        self.finished_pipelines = 0
        self.connect(api=api, broadcast=broadcast)

    def __repr__(self):
        return '<PipelineManager: {} submitted, {} finished>'\
                .format(self.started_pipelines, self.finished_pipelines)

    def start(self, pipeline):
        if pipeline.id is not None:
            raise ValueError('This pipeline was already started')
        request = {'command': 'add pipeline', 'pipeline': pipeline.serialize()}
        self.send_api_request(request)
        result = self.get_api_reply()
        pipeline.started_at = time()
        pipeline_id = str(result['pipeline id'])
        self.started_pipelines += 1
        self.broadcast_subscribe('pipeline finished: id=' + pipeline_id)
        pipeline.id = pipeline_id
        pipeline.finished = False
        self._pipelines[pipeline_id] = pipeline
        return pipeline_id

    def update(self, timeout):
        '''Verify if some pipelines have finished processing

        If there is any pipeline finished, it'll update `PipelineManager`'s
        `finished_pipelines` attribute and will set `finished` to `True` on
        each finished `Pipeline` object.

        You must provide `timeout`, the maximum time this method will hang
        waiting for 'finished pipeline' messages from Router.
        '''
        start_time = time()
        while self.broadcast_poll(self.poll_time) and \
              time() - start_time < timeout:
            message = self.broadcast_receive()
            if message.startswith('pipeline finished: '):
                try:
                    data = message.split(': ')[1].split(', ')
                    pipeline_id = data[0].split('=')[1]
                    duration = float(data[1].split('=')[1])
                except (IndexError, ValueError):
                    continue
                try:
                    pipeline = self._pipelines[pipeline_id]
                except IndexError:
                    continue
                pipeline.duration = duration
                pipeline.finished = True
                self.finished_pipelines += 1
                self.broadcast_unsubscribe('pipeline finished: id=' + \
                                           pipeline_id)

    def finished(self, pipeline):
        '''This method is deprecated. You should use `update` instead.'''
        if pipeline.id is None or pipeline.id not in self._pipelines:
            raise ValueError('This pipeline is not being managed by this '
                             'PipelineMager')
        self.update(self.poll_time * 10)
        return pipeline.finished

    @property
    def pipelines(self):
        return self._pipelines.itervalues()
