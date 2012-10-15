# coding: utf-8

from __future__ import print_function
import unittest
import shlex
import select
from signal import SIGINT, SIGKILL
from time import sleep, time
from subprocess import Popen, PIPE
from uuid import uuid4
import zmq
from psutil import Process, NoSuchProcess
from pypelinin import Job, Pipeline


TIMEOUT = 1500
DEBUG_STDOUT = False
DEBUG_STDERR = False

def _print_debug(name, message):
    print()
    print('----- {} BEGIN -----'.format(name))
    print(message)
    print('----- {} END -----'.format(name))

def _kill(pid, timeout=1.5):
    try:
        process = Process(pid)
    except NoSuchProcess:
        return
    try:
        process.send_signal(SIGINT)
        sleep(timeout)
    except OSError:
        pass
    finally:
        try:
            process.send_signal(SIGKILL)
        except (OSError, NoSuchProcess):
            pass
        process.wait()

class TestPipeliner(unittest.TestCase):
    def setUp(self):
        self.context = zmq.Context()
        self.start_router_sockets()
        self.start_pipeliner_process()
        sleep(1) # wait for subscribe to take effect

    def tearDown(self):
        self.end_pipeliner_process()
        self.close_sockets()
        self.context.term()

    def start_pipeliner_process(self):
        self.pipeliner = Popen(shlex.split('python ./tests/my_pipeliner.py'),
                               stdin=PIPE, stdout=PIPE, stderr=PIPE)
        #TODO: use select and self.fail
        for line in self.pipeliner.stdout.readline():
            if 'main loop' in line:
                break

    def end_pipeliner_process(self):
        try:
            pipeliner_process = Process(self.pipeliner.pid)
        except NoSuchProcess:
            return # was killed

        # kill main process and its children
        children = [process.pid for process in pipeliner_process.get_children()]
        _kill(self.pipeliner.pid, timeout=TIMEOUT / 1000.0)
        for child_pid in children:
            _kill(child_pid, timeout=TIMEOUT / 1000.0)

        # get stdout and stderr
        stdout = self.pipeliner.stdout.read()
        stderr = self.pipeliner.stderr.read()
        if stdout and DEBUG_STDOUT:
            _print_debug('STDOUT', ''.join(stdout))
        if stderr and DEBUG_STDERR:
            _print_debug('STDERR', ''.join(stderr))


    def start_router_sockets(self):
        self.api = self.context.socket(zmq.REP)
        self.broadcast = self.context.socket(zmq.PUB)
        self.api.bind('tcp://*:5555')
        self.broadcast.bind('tcp://*:5556')

    def close_sockets(self):
        self.api.close()
        self.broadcast.close()

    def send_no_pipeline(self):
        self.api.send_json({'pipeline': None})

    def send_pipeline(self, pipeline_definition):
        pipeline = Pipeline(pipeline_definition['graph'],
                            data=pipeline_definition['data'])
        self.api.send_json({'pipeline': pipeline.serialize(),
                            'pipeline id': pipeline_definition['pipeline id']})

    def check_add_job(self):
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'add job' from pipeliner")
        message = self.api.recv_json()
        if message['command'] == 'add job':
            job_id = uuid4().hex
            self.api.send_json({'answer': 'job accepted',
                                'job id': job_id})
            return message, job_id
        elif message['command'] == 'get pipeline':
            self.send_no_pipeline()
            return self.check_add_job()

    def ignore_get_pipeline(self):
        if self.api.poll(TIMEOUT):
            message = self.api.recv_json()
            if message['command'] == 'get pipeline':
                self.send_no_pipeline()
            else:
                self.fail('Should not receive message "{}" '
                          'here.'.format(message))

    def get_api_request(self):
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive message in API channel")
        else:
            message = self.api.recv_json()
            return message


    def test_should_receive_get_pipeline_when_broadcast_new_pipeline(self):
        for i in range(10): #everytime it receives a new pipeline broadcast...
            self.broadcast.send('new pipeline')
            if not self.api.poll(TIMEOUT):
                self.fail("Didn't receive 'get pipeline' from pipeliner")
            message = self.api.recv_json()
            self.send_no_pipeline()
            self.assertEqual(message, {'command': 'get pipeline'})

    def test_should_create_a_job_request_after_getting_a_pipeline(self):
        job_counter = 0
        for index in range(20):
            if index < 10:
                self.broadcast.send('new pipeline')
            if not self.api.poll(TIMEOUT):
                self.fail("Didn't receive 'get pipeline' from pipeliner")
            message = self.api.recv_json()
            if message == {'command': 'get pipeline'}:
                pipeline = {'graph': {Job('Dummy'): None},
                            'data': {'index': index},
                            'pipeline id': uuid4().hex}
                self.send_pipeline(pipeline)
            elif message['command'] == 'add job':
                job_counter += 1
                self.assertEqual(message['worker'], 'Dummy')
                self.api.send_json({'answer': 'job accepted',
                                    'job id': uuid4().hex})
        self.assertEqual(job_counter, 10)

    def test_pipeliner_should_send_pipeline_finished_when_router_sends_job_finished(self):
        self.broadcast.send('job finished: {}'.format(uuid4().hex))
        if self.api.poll(TIMEOUT):
            message = self.api.recv_json()
            if message['command'] != 'get pipeline':
                self.fail('Should not receive any message at this point')
            else:
                self.send_no_pipeline()

        self.broadcast.send('new pipeline')
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        pipeline = {'graph': {Job('Dummy'): None},
                    'data': {}, 'pipeline id': pipeline_id}
        self.send_pipeline(pipeline)

        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        job_id = uuid4().hex
        self.api.send_json({'answer': 'job accepted',
                            'job id': job_id})
        self.broadcast.send('job finished: {}'.format(job_id))

        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        if message['command'] == 'get pipeline':
            self.send_no_pipeline()
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'finished pipeline' from pipeliner")
        message = self.api.recv_json()
        self.assertIn('command', message)
        self.assertIn('pipeline id', message)
        self.assertIn('duration', message)
        self.assertEquals(message['command'], 'pipeline finished')
        self.assertEquals(message['pipeline id'], pipeline_id)

    def test_pipeliner_should_be_able_to_add_jobs_in_sequence(self):
        self.broadcast.send('new pipeline')
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        pipeline_graph = {Job('Dummy'): Job('Dummy2'),
                          Job('Dummy2'): Job('Dummy3')}
        pipeline = {'graph': pipeline_graph,
                    'data': {},
                    'pipeline id': pipeline_id}
        self.send_pipeline(pipeline)
        start_time = time()

        job_workers = []
        finished_job_counter = 0
        while finished_job_counter < 3:
            if not self.api.poll(TIMEOUT):
                self.fail("Didn't receive 'add job' from pipeliner")
            message = self.api.recv_json()
            if message['command'] == 'add job':
                job_id = uuid4().hex
                self.api.send_json({'answer': 'job accepted',
                                    'job id': job_id})
                job_workers.append(message['worker'])
                if self.api.poll(TIMEOUT * 10):
                    message = self.api.recv_json()
                    if message['command'] == 'get pipeline':
                        self.send_no_pipeline()
                    else:
                        self.fail("Should not receive messages at this point")
                finished_job_counter += 1
                self.broadcast.send('job finished: {}'.format(job_id))
            elif message['command'] == 'get pipeline':
                self.send_no_pipeline()
        self.assertEqual(finished_job_counter, 3)
        # then, check order of jobs sent
        self.assertEqual(job_workers, ['Dummy', 'Dummy2', 'Dummy3'])

        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'finished pipeline' from pipelines")
        message = self.api.recv_json()
        end_time = time()
        total_time = end_time - start_time
        self.assertIn('command', message)
        self.assertIn('pipeline id', message)
        self.assertIn('duration', message)
        self.assertEqual(message['command'], 'pipeline finished')
        self.assertEqual(message['pipeline id'], pipeline_id)
        self.assertTrue(message['duration'] <= total_time)

    def test_pipeliner_should_be_able_to_add_jobs_in_parallel(self):
        self.broadcast.send('new pipeline')
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        pipeline = {Job('Dummy'): None, Job('Dummy2'): None,
                    Job('Dummy3'): None}
        pipeline = {'graph': pipeline,
                    'data': {},
                    'pipeline id': pipeline_id}
        self.send_pipeline(pipeline)
        start_time = time()

        job_ids = []
        while len(job_ids) < 3:
            if not self.api.poll(TIMEOUT):
                self.fail("Didn't receive 'add job' from pipeliner")
            message = self.api.recv_json()
            if message['command'] == 'add job':
                job_id = uuid4().hex
                self.api.send_json({'answer': 'job accepted',
                                    'job id': job_id})
                job_ids.append(job_id)
            elif message['command'] == 'get pipeline':
                self.send_no_pipeline()
        for job_id in job_ids:
            self.broadcast.send('job finished: {}'.format(job_id))

        pipeline_finished = False
        while not pipeline_finished:
            if not self.api.poll(TIMEOUT):
                self.fail("Didn't receive 'finished pipeline' from pipelines")
            message = self.api.recv_json()
            if message['command'] == 'get pipeline':
                self.send_no_pipeline()
            else:
                pipeline_finished = True
        end_time = time()
        total_time = end_time - start_time
        self.assertIn('command', message)
        self.assertIn('pipeline id', message)
        self.assertIn('duration', message)
        self.assertEqual(message['command'], 'pipeline finished')
        self.assertEqual(message['pipeline id'], pipeline_id)
        self.assertTrue(message['duration'] <= total_time)

    def test_pipeliner_should_be_able_to_add_jobs_in_sequence_and_parallel_mixed(self):
        self.broadcast.send('new pipeline')
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get pipeline' from pipeliner")
        message = self.api.recv_json()
        pipeline_id = uuid4().hex
        pipeline_graph = {Job('w1'): (Job('w2.1'), Job('w2.2'), Job('w2.3')),
                          (Job('w2.1'), Job('w2.2'), Job('w2.3')): Job('w3')}
        pipeline = {'graph': pipeline_graph,
                    'data': {},
                    'pipeline id': pipeline_id}
        self.send_pipeline(pipeline)
        start_time = time()

        message, job_id = self.check_add_job()
        expected = {'command': 'add job', 'worker': 'w1', 'data': {}}
        self.assertEqual(message, expected)
        self.broadcast.send('job finished: {}'.format(job_id))

        message_1, job_id_2_1 = self.check_add_job()
        message_2, job_id_2_2 = self.check_add_job()
        message_3, job_id_2_3 = self.check_add_job()
        workers = set([message_1['worker'], message_2['worker'],
                       message_3['worker']])
        self.assertEqual(workers, set(['w2.1', 'w2.2', 'w2.3']))

        self.ignore_get_pipeline()
        self.broadcast.send('job finished: {}'.format(job_id_2_1))
        self.broadcast.send('job finished: {}'.format(job_id_2_2))
        self.broadcast.send('job finished: {}'.format(job_id_2_3))

        message, job_id = self.check_add_job()
        self.assertEqual(message['worker'], 'w3')

        end_time = time()
        total_time = end_time - start_time
        self.broadcast.send('job finished: {}'.format(job_id))
        message = self.get_api_request()
        self.assertEqual(message['command'], 'pipeline finished')
        self.assertEqual(message['pipeline id'], pipeline_id)
        self.assertTrue(message['duration'] > total_time)
        self.assertTrue(message['duration'] < 1.5 * total_time)


#TODO: create helper functions for interacting with pipeliner
#TODO: max of pipelines per Pipeliner?
#TODO: handle incorrect pipelines (ignored) - send message to Router
#TODO: move process management helper functions to another module
