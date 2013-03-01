# coding: utf-8

import unittest

from multiprocessing import Pool
from os import unlink
from tempfile import NamedTemporaryFile
from textwrap import dedent
from time import sleep, time
from uuid import uuid4

import zmq

from pypelinin import Job, Pipeline, PipelineManager, PipelineForPipeliner


API_ADDRESS = 'tcp://127.0.0.1:5558'
BROADCAST_ADDRESS = 'tcp://127.0.0.1:5559'

class JobTest(unittest.TestCase):
    def test_worker_name(self):
        self.assertEqual(Job('ABC').worker_name, 'ABC')

    def test_should_start_with_no_data(self):
        self.assertEqual(Job('ABC').data, None)

    def test_repr(self):
        self.assertEqual(repr(Job('ABC')), "<Job worker=ABC>")
        self.assertEqual(repr(Job('ABC', data={'a': 'b'})),
                         "<Job worker=ABC, data=...>")

    def test_equal_not_equal_and_hash(self):
        job_1 = Job('qwe')
        job_2 = Job('qwe')
        job_3 = Job('bla')
        self.assertTrue(job_1 == job_2)
        self.assertTrue(job_2 == job_1)
        self.assertTrue(job_1 != job_3)
        self.assertTrue(job_3 != job_1)
        self.assertEqual(hash(job_1), hash(job_2))
        self.assertNotEqual(hash(job_1), hash(job_3))

    def test_serialize_and_deserialize(self):
        with self.assertRaises(ValueError):
            Job.deserialize({}) # no key 'worker_name'

        job = Job('test')
        expected = tuple({'worker_name': 'test'}.items())
        self.assertEqual(job.serialize(), expected)
        self.assertEqual(Job.deserialize(expected), job)

        job_with_data = Job('testing', data={'python': 42, 'spam': 'eggs'})
        expected_with_data = {'worker_name': 'testing',
                              'data': tuple({'python': 42,
                                             'spam': 'eggs'}.items())}
        expected_with_data = tuple(expected_with_data.items())
        self.assertEqual(job_with_data.serialize(), expected_with_data)
        self.assertEqual(Job.deserialize(expected_with_data), job_with_data)
        self.assertEqual(Job.deserialize(job_with_data.serialize()).serialize(),
                         job_with_data.serialize())

class PipelineTest(unittest.TestCase):
    def test_only_accept_Job_objects(self):
        with self.assertRaises(ValueError):
            Pipeline({'test': 123})

    def test_repr(self):
        result = repr(Pipeline({Job('A'): Job('B'), Job('B'): Job('C')}))
        expected_list = []
        expected_list.append('<Pipeline: A, B, C>')
        expected_list.append('<Pipeline: A, C, B>')
        expected_list.append('<Pipeline: B, A, C>')
        expected_list.append('<Pipeline: B, C, A>')
        expected_list.append('<Pipeline: C, A, B>')
        expected_list.append('<Pipeline: C, B, A>')
        self.assertIn(result, expected_list)

        result = repr(Pipeline({Job('A'): None}, data={'a': 'test'}))
        expected = '<Pipeline: A, data=...>'
        self.assertEqual(expected, result)

    def test_jobs(self):
        result = Pipeline({Job('A'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')],
                           Job('W'): Job('A')}).jobs
        expected = (Job('A'), Job('B'), Job('C'), Job('D'), Job('E'), Job('W'),
                    Job('Z'))
        self.assertEqual(set(result), set(expected))

    def test_get_starters(self):
        result = Pipeline({Job('A'): []}).starters
        expected = (Job('A'),)
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [], Job('B'): []}).starters
        expected = (Job('A'), Job('B'))
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')], Job('B'): []}).starters
        expected = (Job('A'),)
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')]}).starters
        expected = (Job('A'), Job('Z'))
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): Job('D')}).starters
        expected = [Job('A'), Job('B'), Job('C')]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                           Job('E'): (Job('B'), Job('F'))}).starters
        expected = (Job('A'), Job('C'), Job('E'))
        self.assertEqual(set(result), set(expected))

    def test_normalize(self):
        result = Pipeline({Job('A'): Job('B')})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')]})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'),): (Job('B'),)})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('C')): Job('B')})._graph
        expected = [(Job('A'), Job('B')), (Job('C'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        graph = {(Job('A'), Job('C')): [Job('B'), Job('D'), Job('E')]}
        result = Pipeline(graph)._graph
        expected = [(Job('A'), Job('B')), (Job('A'), Job('D')),
                    (Job('A'), Job('E')), (Job('C'), Job('B')),
                    (Job('C'), Job('D')),
                    (Job('C'), Job('E'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('ABC'): []})._graph # problem here if use string
        expected = [(Job('ABC'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [], Job('B'): []})._graph
        expected = [(Job('A'), None), (Job('B'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')], Job('B'): []})._graph
        expected = [(Job('A'), Job('B')), (Job('B'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('QWE'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')]})._graph
        expected = [(Job('QWE'), Job('B')), (Job('B'), Job('C')),
                    (Job('B'), Job('D')), (Job('B'), Job('E')),
                    (Job('Z'), Job('W'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')]})._graph
        expected = [(Job('A'), Job('D')), (Job('B'), Job('D')),
                    (Job('C'), Job('D'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                           Job('E'): (Job('B'), Job('F'))})._graph
        expected = [(Job('A'), Job('D')), (Job('B'), Job('D')),
                    (Job('C'), Job('D')), (Job('E'), Job('B')),
                    (Job('E'), Job('F'))]
        self.assertEqual(set(result), set(expected))

    def test_validate_graph(self):
        #should have at least one starter node
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): Job('A')})
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): [Job('B')], Job('B'): [Job('A')]})

        #should not have cycles
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): [Job('B')], Job('B'): [Job('C')],
                      Job('C'): [Job('B')]})
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): [Job('B')], Job('B'): [Job('C')],
                      Job('C'): [Job('D')], Job('D'): [Job('B')]})

    def test_str_and_save_dot(self):
        pipeline = Pipeline({Job('A'): Job('B'), Job('C'): None})
        result = str(pipeline)
        expected = dedent('''
        digraph graphname {
            "A";
            "C";
            "B";

            "A" -> "B";
            "C" -> "(None)";
        }
        ''').strip()
        self.assertEqual(result, expected)

        pipeline = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                             Job('E'): (Job('B'), Job('F'))})
        result = str(pipeline)
        expected = dedent('''
        digraph graphname {
            "A";
            "C";
            "B";
            "E";
            "D";
            "F";

            "A" -> "D";
            "B" -> "D";
            "C" -> "D";
            "E" -> "B";
            "E" -> "F";
        }
        ''').strip()

        self.assertEqual(result, expected)
        temp_file = NamedTemporaryFile(delete=False)
        temp_file.close()
        pipeline.save_dot(temp_file.name)
        temp_file = open(temp_file.name)
        file_contents = temp_file.read()
        temp_file.close()
        self.assertEqual(expected + '\n', file_contents)
        unlink(temp_file.name)

    def test_pipeline_should_propagate_data_among_jobs(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: job_2, job_2: job_3}, data=pipeline_data)
        self.assertEqual(pipeline.data, pipeline_data)
        self.assertEqual(job_1.data, pipeline_data)
        self.assertEqual(job_2.data, pipeline_data)
        self.assertEqual(job_3.data, pipeline_data)
        self.assertEqual(job_1.pipeline, pipeline)
        self.assertEqual(job_2.pipeline, pipeline)
        self.assertEqual(job_3.pipeline, pipeline)

    def test_pipeline_add_finished_job(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = PipelineForPipeliner({job_1: job_2, job_2: job_3},
                                        data=pipeline_data)
        job_4 = Job('w4')

        self.assertFalse(pipeline.finished_job(job_1))
        self.assertFalse(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_1)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertFalse(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_2)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertTrue(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_3)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertTrue(pipeline.finished_job(job_2))
        self.assertTrue(pipeline.finished_job(job_3))

        with self.assertRaises(ValueError):
            pipeline.add_finished_job(job_4) # job not in pipeline
        with self.assertRaises(RuntimeError):
            pipeline.add_finished_job(job_3) # already finished

    def test_pipeline_finished(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = PipelineForPipeliner({job_1: job_2, job_2: job_3},
                                        data=pipeline_data)

        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_1)
        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_2)
        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_3)
        self.assertTrue(pipeline.finished())

    def test_default_attributes(self):
        pipeline = Pipeline({Job('test'): None})
        self.assertEqual(pipeline.data, None)
        self.assertEqual(pipeline.id, None)
        self.assertEqual(pipeline.jobs, (Job('test'),))
        self.assertEqual(pipeline.sent_jobs, set())

    def test_available_jobs(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = PipelineForPipeliner({job_1: job_2, job_2: job_3},
                                        data=pipeline_data)

        expected = [job_1]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_1)
        expected = [job_2]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_2)
        expected = [job_3]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_3)
        self.assertEqual(pipeline.available_jobs(), set())

        job_4, job_5, job_6, job_7 = Job('w4'), Job('w5'), Job('w6'), Job('w7')
        job_8, job_9, job_10 = Job('8'), Job('9'), Job('10')
        job_11, job_12, job_13 = Job('11'), Job('12'), Job('13')
        job_14, job_15, job_16 = Job('14'), Job('15'), Job('16')
        pipeline_data = {'python': 42}
        pipeline = PipelineForPipeliner({job_1: (job_2, job_3),
                                         job_2: (job_4, job_16),
                                         job_3: job_4,
                                         job_4: job_5,
                                         job_5: (job_6, job_7, job_8, job_9),
                                         (job_6, job_7, job_8): job_10,
                                         (job_10, job_11): (job_12, job_13,
                                                            job_14),
                                         job_15: None},
                            data=pipeline_data)

        expected = [job_1, job_11, job_15]
        self.assertEqual(pipeline.available_jobs(), set(expected))
        self.assertEqual(pipeline.available_jobs(), set(pipeline.starters))

        pipeline.add_finished_job(job_1)
        expected = [job_11, job_15, job_2, job_3]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_2)
        expected = [job_11, job_15, job_3, job_16]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_3)
        expected = [job_11, job_15, job_4, job_16]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_16)
        expected = [job_11, job_15, job_4]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_4)
        expected = [job_11, job_15, job_5]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_11)
        expected = [job_15, job_5]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_5)
        expected = [job_15, job_6, job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_6)
        expected = [job_15, job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_15)
        expected = [job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_7)
        expected = [job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_9)
        expected = [job_8]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_8)
        expected = [job_10]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_10)
        expected = [job_12, job_13, job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_13)
        expected = [job_12, job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_12)
        expected = [job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_14)
        expected = []
        self.assertEqual(pipeline.available_jobs(), set(expected))

        self.assertTrue(pipeline.finished())

    def test_serialize(self):
        job_1, job_2, job_3, job_4 = (Job('spam'), Job('eggs'), Job('ham'),
                                      Job('python'))
        pipeline = Pipeline({job_1: job_2, job_2: (job_3, job_4)})
        result = pipeline.serialize()
        expected = {'graph': ((job_1.serialize(), job_2.serialize()),
                              (job_2.serialize(), job_3.serialize()),
                              (job_2.serialize(), job_4.serialize())),
                    'data': None}
        expected = tuple(expected.items())

        result = dict(result)
        expected = dict(expected)
        result['graph'] = dict(result['graph'])
        expected['graph'] = dict(expected['graph'])
        self.assertEqual(result, expected)

        pipeline = Pipeline({job_1: job_2}, data={'python': 42})
        self.assertEqual(pipeline, Pipeline.deserialize(pipeline.serialize()))

    def test_deserialize(self):
        job_1, job_2, job_3, job_4, job_5 = (Job('spam'), Job('eggs'),
                                             Job('ham'), Job('python'),
                                             Job('answer_42'))
        pipeline = Pipeline({job_1: job_2, job_2: (job_3, job_4), job_5: None},
                            data={'key': 42})
        serialized = pipeline.serialize()
        new_pipeline = Pipeline.deserialize(serialized)
        self.assertEqual(pipeline, new_pipeline)
        self.assertEqual(serialized, new_pipeline.serialize())

    def test_equal_not_equal_hash(self):
        job_1, job_2, job_3, job_4 = (Job('spam'), Job('eggs'), Job('ham'),
                                      Job('python'))
        pipeline_1 = Pipeline({job_1: job_2, job_2: (job_3, job_4)})
        pipeline_2 = Pipeline({job_1: job_2, job_2: (job_3, job_4)})
        pipeline_3 = Pipeline({job_1: job_2, job_2: job_3, job_3: job_4})
        self.assertTrue(pipeline_1 == pipeline_2)
        self.assertTrue(pipeline_2 == pipeline_1)
        self.assertTrue(pipeline_1 != pipeline_3)
        self.assertTrue(pipeline_3 != pipeline_1)

        my_set = set([pipeline_1, pipeline_2, pipeline_3]) #test __hash__
        self.assertIn(pipeline_1, my_set)
        self.assertIn(pipeline_2, my_set)
        self.assertIn(pipeline_3, my_set)

        pipeline_with_data = Pipeline({job_1: job_2, job_2: (job_3, job_4)},
                                      data={'python': 42})
        pipeline_with_data_2 = Pipeline({job_1: job_2, job_2: (job_3, job_4)},
                                        data={'python': 42})
        self.assertTrue(pipeline_with_data == pipeline_with_data_2)
        self.assertTrue(pipeline_with_data_2 == pipeline_with_data)
        self.assertTrue(pipeline_1 != pipeline_with_data)
        self.assertTrue(pipeline_with_data != pipeline_1)

def run_in_parallel(function, args=tuple()):
    pool = Pool(processes=1)
    result = pool.apply_async(function, args)
    return result, pool

def send_pipeline():
    pipeline = Pipeline({Job(u'worker_1'): Job(u'worker_2'),
                         Job(u'worker_2'): Job(u'worker_3')})
    pipeline_manager = PipelineManager(api=API_ADDRESS,
                                       broadcast=BROADCAST_ADDRESS)
    before = pipeline.id
    pipeline_id = pipeline_manager.start(pipeline)
    pipeline_manager.disconnect()
    return before, pipeline_id, pipeline.id

def send_pipeline_and_wait_finished():
    pipeline_manager = PipelineManager(api=API_ADDRESS,
                                       broadcast=BROADCAST_ADDRESS)
    pipelines = []
    for i in range(10):
        pipeline = Pipeline({Job(u'worker_1'): Job(u'worker_2'),
                             Job(u'worker_2'): Job(u'worker_3')},
                            data={'index': i})
        pipeline_manager.start(pipeline)
        pipelines.append(pipeline)
    assert pipeline_manager.started_pipelines == 10
    assert pipeline_manager.finished_pipelines == 0
    start = time()
    pipeline_manager.finished(pipelines[0]) # only for testing this method
    while pipeline_manager.finished_pipelines < pipeline_manager.started_pipelines:
        pipeline_manager.update(0.5)
    end = time()
    pipeline_manager.disconnect()
    return {'duration': pipeline.duration, 'real_duration': end - start,
            'finished_pipelines': pipeline_manager.finished_pipelines,
            'started_pipelines': pipeline_manager.started_pipelines}

def verify_PipelineManager_exceptions():
    pipeline_1 = Pipeline({Job(u'worker_1'): Job(u'worker_2'),
                           Job(u'worker_2'): Job(u'worker_3')})
    pipeline_2 = Pipeline({Job(u'worker_1'): Job(u'worker_2')})
    pipeline_manager = PipelineManager(api=API_ADDRESS,
                                       broadcast=BROADCAST_ADDRESS)
    pipeline_manager.start(pipeline_1)
    raise_1, raise_2 = False, False
    try:
        pipeline_manager.start(pipeline_1)
    except ValueError:
        raise_1 = True
    try:
        pipeline_manager.finished(pipeline_2)
    except ValueError:
        raise_2 = True

    pipeline_manager.disconnect()
    return {'raise_1': raise_1, 'raise_2': raise_2,
            'started_at': pipeline_1.started_at}

class PipelineManagerTest(unittest.TestCase):
    def setUp(self):
        self.context = zmq.Context()
        self.start_router_sockets()
        self.pipeline = Pipeline({Job(u'worker_1'): Job(u'worker_2'),
                                  Job(u'worker_2'): Job(u'worker_3')})

    def tearDown(self):
        self.close_sockets()
        self.context.term()

    def start_router_sockets(self):
        self.api = self.context.socket(zmq.REP)
        self.broadcast = self.context.socket(zmq.PUB)
        self.api.bind(API_ADDRESS)
        self.broadcast.bind(BROADCAST_ADDRESS)

    def close_sockets(self):
        self.api.close()
        self.broadcast.close()

    def test_repr(self):
        pipeline_manager = PipelineManager(api=API_ADDRESS,
                                           broadcast=BROADCAST_ADDRESS)
        pipeline_ids = [uuid4().hex for i in range(10)]
        pipeline_ids_copy = pipeline_ids[:]
        pipeline_manager.send_api_request = lambda x: None
        pipeline_manager.get_api_reply = \
                lambda: {'pipeline id': pipeline_ids.pop()}
        pipelines = [Pipeline({Job('A', data={'index': i}): Job('B')}) \
                     for i in range(10)]
        for pipeline in pipelines:
            pipeline_manager.start(pipeline)

        result = repr(pipeline_manager)
        self.assertEqual(result, '<PipelineManager: 10 submitted, 0 finished>')

        messages = ['pipeline finished: id={}, duration=0.1'.format(pipeline_id)
                    for pipeline_id in pipeline_ids_copy[:3]]
        poll = [False, True, True, True]
        def new_poll(timeout):
            return poll.pop()
        def new_broadcast_receive():
            return messages.pop()
        pipeline_manager.broadcast_poll = new_poll
        pipeline_manager.broadcast_receive = new_broadcast_receive
        pipeline_manager.update(0.1)
        result = repr(pipeline_manager)
        self.assertEqual(result, '<PipelineManager: 10 submitted, 3 finished>')

    def test_should_send_add_pipeline_with_serialized_pipeline(self):
        result, pool = run_in_parallel(send_pipeline)
        message = self.api.recv_json()
        received = Pipeline.deserialize(message['pipeline']).serialize()
        expected = self.pipeline.serialize()
        self.assertEqual(set(message.keys()), set(['command', 'pipeline']))
        self.assertEqual(message['command'], 'add pipeline')
        self.assertEqual(received, expected)

        pipeline_id = uuid4().hex
        self.api.send_json({'answer': 'pipeline accepted',
                            'pipeline id': pipeline_id})
        result.get()
        pool.terminate()

    def test_should_save_pipeline_id_on_pipeline_object(self):
        result, pool = run_in_parallel(send_pipeline)
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        self.api.send_json({'answer': 'pipeline accepted',
                            'pipeline id': pipeline_id})
        received = result.get()
        pool.terminate()
        self.assertEqual(received, (None, pipeline_id, pipeline_id))

    def test_should_subscribe_to_broadcast_to_wait_for_finished_pipeline(self):
        result, pool = run_in_parallel(send_pipeline_and_wait_finished)
        pipeline_ids = []
        for i in range(10):
            message = self.api.recv_json()
            pipeline_id = uuid4().hex
            self.api.send_json({'answer': 'pipeline accepted',
                                'pipeline id': pipeline_id})
            pipeline_ids.append(pipeline_id)
        sleep(1)
        for pipeline_id in pipeline_ids:
            self.broadcast.send('pipeline finished: id={}, duration=1.23456'\
                                .format(pipeline_id))
        received = result.get()
        pool.terminate()
        self.assertEqual(received['duration'], 1.23456)
        self.assertTrue(received['real_duration'] > 1)
        self.assertTrue(received['finished_pipelines'], 10)
        self.assertTrue(received['started_pipelines'], 10)

    def test_should_raise_ValueError_in_some_cases(self):
        result, pool = run_in_parallel(verify_PipelineManager_exceptions)
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        self.api.send_json({'answer': 'pipeline accepted',
                            'pipeline id': pipeline_id})
        start_time = time()
        received = result.get()
        pool.terminate()
        self.assertTrue(received['raise_1'])
        self.assertTrue(received['raise_2'])
        started_at = received['started_at']
        self.assertTrue(start_time - 0.1 <= started_at <= start_time + 0.1)

    def test_should_return_all_pipelines(self):
        pipeline_manager = PipelineManager(api=API_ADDRESS,
                                           broadcast=BROADCAST_ADDRESS)
        pipeline_manager.send_api_request = lambda x: None
        pipeline_manager.get_api_reply = lambda: {'pipeline id': uuid4().hex}
        iterations = 10
        pipelines = []
        for i in range(iterations):
            pipeline = Pipeline({Job(u'worker_1'): Job(u'worker_2'),
                                 Job(u'worker_2'): Job(u'worker_3')},
                                data={'index': i})
            pipeline_manager.start(pipeline)
            pipelines.append(pipeline)
        self.assertEqual(set(pipeline_manager.pipelines), set(pipelines))
