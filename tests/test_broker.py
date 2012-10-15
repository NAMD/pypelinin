# coding: utf-8

from __future__ import print_function
import json
import os
import unittest
import shlex
import select
import tempfile
from signal import SIGINT, SIGKILL
from time import sleep, time
from subprocess import Popen, PIPE
from multiprocessing import cpu_count
from uuid import uuid4
import zmq
from psutil import Process, NoSuchProcess
from utils import default_config


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

class TestBroker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cpus = cpu_count()
        cls.monitoring_interval = 60
        cls.config = default_config

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.context = zmq.Context()
        self.start_router_sockets()
        self.start_broker_process()

    def tearDown(self):
        self.end_broker_process()
        self.close_sockets()
        self.context.term()

    def start_broker_process(self):
        #TODO: call process passing a configuration file
        self.broker = Popen(shlex.split('python ./tests/my_broker.py'),
                            stdin=PIPE, stdout=PIPE, stderr=PIPE)
        #TODO: use select and self.fail
        for line in self.broker.stdout.readline():
            if 'main loop' in line:
                break

    def end_broker_process(self):
        try:
            broker_process = Process(self.broker.pid)
        except NoSuchProcess:
            return # was killed
        # get stdout and stderr
        select_config = [self.broker.stdout, self.broker.stderr], [], [], 0.1
        stdout, stderr = [], []
        result = select.select(*select_config)
        while any(result):
            if result[0]:
                stdout.append(result[0][0].readline())
            if result[1]:
                stderr.append(result[1][0].readline())
            result = select.select(*select_config)
        if stdout and DEBUG_STDOUT:
            _print_debug('STDOUT', ''.join(stdout))
        if stderr and DEBUG_STDERR:
            _print_debug('STDERR', ''.join(stderr))

        # kill main process and its children
        children = [process.pid for process in broker_process.get_children()]
        _kill(self.broker.pid, timeout=TIMEOUT / 1000.0)
        for child_pid in children:
            _kill(child_pid, timeout=TIMEOUT / 1000.0)

    def start_router_sockets(self):
        self.api = self.context.socket(zmq.REP)
        self.broadcast = self.context.socket(zmq.PUB)
        self.api.bind('tcp://*:5555')
        self.broadcast.bind('tcp://*:5556')

    def close_sockets(self):
        self.api.close()
        self.broadcast.close()

    def receive_get_configuration_and_send_it_to_broker(self):
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get configuration' from broker")
        message = self.api.recv_json()
        self.config['monitoring interval'] = self.monitoring_interval
        self.api.send_json(self.config)
        self.assertEqual(message, {'command': 'get configuration'})

    def receive_get_job_and_send_it_to_broker(self, job=None):
        if not self.api.poll(TIMEOUT):
            self.fail("Didn't receive 'get job' from broker")
        message = self.api.recv_json()
        if job is None:
            job = {'worker': 'Dummy', 'data': {'id': '1'}, 'job id': '2'}
        self.api.send_json(job)
        self.assertEqual(message, {'command': 'get job'})

    def broker_should_be_quiet(self):
        sleep(TIMEOUT / 1000.0)
        with self.assertRaises(zmq.ZMQError):
            self.api.recv_json(zmq.NOBLOCK)

    def send_and_receive_jobs(self, jobs, wait_finished_job=False):
        my_jobs = list(jobs)
        finished_jobs = [True for job in my_jobs]
        messages = []
        condition = True
        while condition:
            if not self.api.poll(3 * TIMEOUT):
                self.fail("Didn't receive 'get job' from broker")
            msg = self.api.recv_json()
            messages.append(msg)
            if msg['command'] == 'get job':
                if len(my_jobs):
                    job = my_jobs.pop(0)
                else:
                    job = {'worker': None}
                if 'job id' not in job:
                    job['job id'] = uuid4().hex
                self.api.send_json(job)
            elif msg['command'] == 'job finished':
                self.api.send_json({'answer': 'good job!'})
                finished_jobs.pop()
            condition = len(my_jobs) or \
                        (wait_finished_job and len(finished_jobs))
        return messages

    def test_should_ask_for_configuration_on_start(self):
        self.receive_get_configuration_and_send_it_to_broker()
        self.send_and_receive_jobs([{'worker': None}])
        # it's necessary to send a job to wait for broker enter on run()

    def test_should_ask_for_a_job_after_configuration(self):
        self.receive_get_configuration_and_send_it_to_broker()
        job = {'worker': 'Dummy', 'data': {'id': '1'}, 'job id': '2'}
        self.send_and_receive_jobs([job])

    def test_should_send_get_job_just_after_router_broadcast_new_job(self):
        self.receive_get_configuration_and_send_it_to_broker()
        self.send_and_receive_jobs([{'worker': None}])
        self.broker_should_be_quiet()
        self.broadcast.send('new job')
        self.send_and_receive_jobs([{'worker': None}]) # just kidding! :D

    def test_should_send_finished_job_when_asked_to_run_dummy_worker(self):
        jobs = []
        for i in range(self.cpus):
            jobs.append({'worker': 'Dummy', 'data': {'id': 'xpto'},
                         'job id': i})
        self.receive_get_configuration_and_send_it_to_broker()
        messages = self.send_and_receive_jobs(jobs, wait_finished_job=True)
        finished_jobs = 0
        for message in messages:
            if message['command'] == 'job finished':
                finished_jobs += 1
        self.assertEqual(finished_jobs, self.cpus)
        self.assertEqual(self.api.recv_json(), {'command': 'get job'})
        self.api.send_json({'worker': None})
        self.broker_should_be_quiet()

    def test_should_start_worker_process_even_if_no_job(self):
        self.receive_get_configuration_and_send_it_to_broker()
        broker_pid = self.broker.pid
        children_pid = [process.pid for process in \
                        Process(broker_pid).get_children()]
        self.assertEqual(len(children_pid), self.cpus)

    def test_should_kill_workers_processes_when_receive_SIGINT(self):
        self.receive_get_configuration_and_send_it_to_broker()
        self.send_and_receive_jobs([{'worker': None}])
        broker_pid = self.broker.pid
        children_pid = [process.pid for process in \
                        Process(broker_pid).get_children()]
        self.end_broker_process()
        sleep(0.5 * (self.cpus + 1)) # cpu_count + 1 processes
        for child_pid in children_pid:
            with self.assertRaises(NoSuchProcess):
                worker_process = Process(child_pid)
        with self.assertRaises(NoSuchProcess):
            broker_process = Process(broker_pid)

    def test_should_reuse_the_same_workers_processes_for_all_jobs(self):
        #TODO: maybe it can use new or different worker processes depending on
        #broker's configuration
        self.receive_get_configuration_and_send_it_to_broker()
        broker_pid = self.broker.pid
        children_pid_before = [process.pid for process in \
                               Process(broker_pid).get_children()]
        job = {'worker': 'Dummy', 'data': {'data': {}},}
        jobs = [job] * self.cpus
        self.send_and_receive_jobs(jobs, wait_finished_job=True)
        children_pid_after = [process.pid for process in \
                              Process(broker_pid).get_children()]
        self.broadcast.send('new job')
        self.send_and_receive_jobs(jobs, wait_finished_job=True)
        children_pid_after_2 = [process.pid for process in \
                                Process(broker_pid).get_children()]
        self.assertEqual(children_pid_before, children_pid_after)
        self.assertEqual(children_pid_before, children_pid_after_2)

    def test_should_return_time_spent_by_each_job(self):
        sleep_time = 1.43
        job = {'worker': 'Snorlax', 'data': {'sleep-for': sleep_time},}
        jobs = [job] * self.cpus
        self.receive_get_configuration_and_send_it_to_broker()
        start_time = time()
        messages = self.send_and_receive_jobs(jobs, wait_finished_job=True)
        end_time = time()
        total_time = end_time - start_time
        counter = 0
        for message in messages:
            if message['command'] == 'job finished':
                counter += 1
                self.assertIn('duration', message)
                self.assertTrue(sleep_time < message['duration'])
        self.assertEqual(len(jobs), counter)
        self.assertTrue(total_time > sleep_time)

    def test_should_save_monitoring_information_regularly(self):
        self.monitoring_interval = 0.5
        self.receive_get_configuration_and_send_it_to_broker()
        self.send_and_receive_jobs([{'worker': None}])
        sleep((self.monitoring_interval + 0.05 + 0.2) * 3)
        # 0.05 = default broker poll time, 0.2 = some overhead
        monitoring_file = open('/tmp/broker-monitoring')
        self.assertEqual(monitoring_file.read().count('\n'), 3)

    def test_should_save_monitoring_information(self):
        self.monitoring_interval = 0.5
        self.receive_get_configuration_and_send_it_to_broker()
        self.send_and_receive_jobs([{'worker': None}])
        sleep(self.monitoring_interval + 0.05 + 0.2)
        # 0.05 = default broker poll time, 0.2 = some overhead
        monitoring_file = open('/tmp/broker-monitoring')
        info = json.loads(monitoring_file.readline().strip())

        self.assertIn('host', info)
        self.assertIn('processes', info)

        needed_host_keys = ['cpu', 'memory', 'network', 'storage', 'uptime']
        for key in needed_host_keys:
            self.assertIn(key, info['host'])

        needed_cpu_keys = ['cpu percent', 'number of cpus']
        for key in needed_cpu_keys:
            self.assertIn(key, info['host']['cpu'])

        needed_memory_keys = ['buffers', 'cached', 'free', 'free virtual',
                              'percent', 'real free', 'real percent',
                              'real used', 'total', 'total virtual', 'used',
                              'used virtual']
        for key in needed_memory_keys:
            self.assertIn(key, info['host']['memory'])

        self.assertIn('cluster ip', info['host']['network'])
        self.assertIn('interfaces', info['host']['network'])
        first_interface = info['host']['network']['interfaces'].keys()[0]
        interface_info = info['host']['network']['interfaces'][first_interface]
        needed_interface_keys = ['bytes received', 'bytes sent',
                                 'packets received', 'packets sent']
        for key in needed_interface_keys:
            self.assertIn(key, interface_info)

        first_partition = info['host']['storage'].keys()[0]
        partition_info = info['host']['storage'][first_partition]
        needed_storage_keys = ['file system', 'mount point', 'percent used',
                               'total bytes', 'total free bytes',
                               'total used bytes']
        for key in needed_storage_keys:
            self.assertIn(key, partition_info)

        self.assertEqual(len(info['processes']), self.cpus + 1)
        needed_process_keys = ['cpu percent', 'pid', 'resident memory',
                               'virtual memory', 'type', 'started at']
        process_info = info['processes'][0]
        for key in needed_process_keys:
            self.assertIn(key, process_info)

    def test_should_insert_monitoring_information_about_workers(self):
        self.monitoring_interval = 0.5
        self.receive_get_configuration_and_send_it_to_broker()
        jobs = []
        start_time = time()
        for i in range(self.cpus):
            jobs.append({'worker': 'Snorlax', 'data': {'sleep-for': 100,
                                                       'data': {'id': 143}}})
        self.send_and_receive_jobs(jobs)
        end_time = time()
        sleep(self.monitoring_interval * 3) # wait for broker to save info
        monitoring_file = open('/tmp/broker-monitoring')
        last_line = monitoring_file.read().split('\n')[-2].strip()
        monitoring_info = json.loads(last_line)
        self.assertEqual(len(monitoring_info['processes']), self.cpus + 1)

        needed_process_keys = ['cpu percent', 'pid', 'resident memory', 'type',
                               'virtual memory', 'started at']
        for process in monitoring_info['processes']:
            for key in needed_process_keys:
                self.assertIn(key, process)

        broker_process = monitoring_info['processes'][0]
        self.assertEqual(broker_process['number of workers'], self.cpus)
        self.assertEqual(broker_process['active workers'], self.cpus)
        self.assertEqual(broker_process['type'], 'broker')
        self.assertTrue(start_time - 3 < broker_process['started at'] < \
                end_time + 3)
        for process in monitoring_info['processes'][1:]:
            self.assertEqual(process['data'],
                             {'sleep-for': 100, 'data': {'id': 143}})
            self.assertTrue(start_time - 3 < process['started at'] < \
                    end_time + 3)
            self.assertEqual(process['type'], 'worker')
            self.assertEqual(process['worker'], 'Snorlax')

    def test_if_broker_calls_save_and_retrieve_methods_from_store(self):
        input_file = tempfile.NamedTemporaryFile(delete=False)
        input_file.write('Answer: 42.')
        input_file.close()
        self.receive_get_configuration_and_send_it_to_broker()
        jobs = [{'worker': 'Upper', 'data': {'filename': input_file.name}}]
        messages = self.send_and_receive_jobs(jobs, wait_finished_job=True)
        result_filename = input_file.name + '.result'
        requires_filename = input_file.name + '.requires'
        with open(result_filename, 'r') as fp:
            contents = fp.read()
        self.assertEqual(contents, 'ANSWER: 42.')
        with open(requires_filename, 'r') as fp:
            contents = fp.read()
        self.assertEqual(contents, "['text']")
        os.unlink(input_file.name)
        os.unlink(result_filename)
        os.unlink(requires_filename)
