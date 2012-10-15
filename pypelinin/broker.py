#!/usr/bin/env python
# coding: utf-8

from importlib import import_module
from multiprocessing import Process, Pipe, cpu_count
from os import kill, getpid
from time import sleep, time
from signal import SIGKILL
from . import Client
from .monitoring import (get_host_info, get_outgoing_ip, get_process_info)


def worker_wrapper(pipe, workers_module_name):
    #TODO: should receive the document or database's configuration?
    #      Note that if a worker should process a big document or an entire
    #      corpus, it's better to received database's configuration and pass to
    #      worker only an lazy iterator for the collection (pymongo's cursor)
    #TODO: create documentation about object type returned by worker (strings
    #      must be unicode)
    #TODO: add the possibility to create workers that are executables (they
    #      receive data as JSON in stdin and put the result as JSON in stdout),
    #      so we can create workers in C, Perl, Ruby etc.
    #TODO: should get any exception information and send it to broker signaling
    #      'job failed' and sending the traceback


    try:
        workers_module = import_module(workers_module_name)
    except:
        pipe.send({'command': 'error'})
        #TODO: handle this on broker
    else:
        workers = {}
        for worker in workers_module.__all__:
            workers[worker] = getattr(workers_module, worker)()
        try:
            while True:
                message = pipe.recv()
                if message['command'] == 'exit':
                    break
                elif message['command'] == 'execute job':
                    worker_name = message['worker']
                    data = message['worker_input']
                    try:
                        result = workers[worker_name].process(data)
                    except Exception as e:
                        result = {'_error': True, '_exception': e}
                        #TODO: handle this on broker
                    finally:
                        pipe.send(result)
        except KeyboardInterrupt:
            pass

class WorkerPool(object):
    #TODO: test it!

    def __init__(self, number_of_workers, workers):
        self.workers = []
        self.number_of_workers = number_of_workers
        for i in range(number_of_workers):
            self.workers.append(Worker(workers))

    def __len__(self):
        return len(self.workers)

    def available(self):
        return [worker.working for worker in self.workers].count(False)

    def working(self):
        return [worker.working for worker in self.workers].count(True)

    def start_job(self, job_info):
        '''
        job_info = {'worker': 'name as string', 'worker_input': {...data...},
                    'data': {...data...}}
        '''
        for worker in self.workers:
            if not worker.working:
                break
        else:
            return False
        return worker.start_job(job_info)

    def end_processes(self):
        for worker in self.workers:
            worker.end()

    def kill_processes(self):
        for worker in self.workers:
            try:
                kill(worker.pid, SIGKILL)
            except OSError:
                pass
        for worker in self.workers:
            worker.process.join()

class Worker(object):
    #TODO: test it!

    def __init__(self, workers):
        parent_connection, child_connection = Pipe()
        self.parent_connection = parent_connection
        self.child_connection = child_connection
        self.start_time = time()
        #TODO: is there any way to *do not* connect stdout/stderr?
        self.process = Process(target=worker_wrapper,
                               args=(child_connection, workers))
        self.process.start()
        self.pid = self.process.pid
        self.working = False
        self.job_info = None

    def start_job(self, job_info):
        if self.working:
            return False
        message = {'command': 'execute job',
                   'worker': job_info['worker'],
                   'worker_input': job_info['worker_input'],}
        self.parent_connection.send(message)
        self.job_info = job_info
        self.job_info['start time'] = time()
        self.working = True
        return True

    def __repr__(self):
        return ('<Worker(pid={}, start_time={})>'.format(self.pid,
                self.start_time))

    def get_result(self):
        if not self.finished_job():
            return None
        result = self.parent_connection.recv()
        self.job_info = None
        self.working = False
        return result

    def finished_job(self):
        return self.parent_connection.poll()

    def end(self):
        self.parent_connection.send({'command': 'exit'})
        self.parent_connection.close()

class Broker(Client):
    #TODO: validate all received data (types, keys etc.)
    #TODO: use log4mongo (?)

    def __init__(self, api, broadcast, store_class, workers, logger,
                 logger_name='Broker', poll_time=50,
                 number_of_workers=cpu_count()):
        super(Broker, self).__init__()
        self._api_address = api
        self._broadcast_address = broadcast
        self.logger = logger
        self.poll_time = poll_time
        self.last_time_saved_monitoring_information = 0
        self.StoreClass = store_class

        self.number_of_workers = number_of_workers
        self.pid = getpid()
        self.logger.info('Starting worker processes')
        self.workers = workers
        self.workers_module = import_module(workers)
        self.available_workers = self.workers_module.__all__
        self.worker_requirements = {}
        for worker in self.available_workers:
            try:
                WorkerClass = getattr(self.workers_module, worker)
            except AttributeError:
                raise RuntimeError("Could not find worker '{}'".format(worker))
            self.worker_requirements[worker] = getattr(WorkerClass, 'requires',
                                                       [])
        self.worker_pool = WorkerPool(self.number_of_workers, self.workers)
        self.logger.info('Broker started')

    def request(self, message):
        self.send_api_request(message)
        self.logger.info('[API] Request to router: {}'.format(message))

    def get_reply(self):
        message = self.get_api_reply()
        self.logger.info('[API] Reply from router: {}'.format(message))
        return message

    def get_configuration(self):
        self.request({'command': 'get configuration'})
        self._config = self.get_reply()

    def connect_to_router(self):
        self.logger.info('Trying to connect to router...')
        self.connect(api=self._api_address, broadcast=self._broadcast_address)
        api_host, api_port = self._api_address.split('//')[1].split(':')
        self.ip = get_outgoing_ip((api_host, int(api_port)))

    def save_monitoring_information(self):
        #TODO: should we send average measures insted of instant measures of
        #      some measured variables?
        #TODO: timestamp sent should be UTC
        host_info = get_host_info()
        host_info['network']['cluster ip'] = self.ip
        broker_process = get_process_info(self.pid)
        broker_process['type'] = 'broker'
        broker_process['number of workers'] = len(self.worker_pool)
        broker_process['active workers'] = self.worker_pool.working()
        processes = [broker_process]
        for worker in self.worker_pool.workers:
            process_info = get_process_info(worker.pid)
            process_info['type'] = 'worker'
            if worker.working:
                process_info['worker'] = worker.job_info['worker']
                process_info['data'] = worker.job_info['data']
            processes.append(process_info)
        data = {'host': host_info, 'timestamp': time(), 'processes': processes}
        try:
            self._store.save_monitoring(data)
        except Exception as e:
            #TODO: what to do?
            self.logger.error('Could not save monitoring information into '
                              'store with parameters: {}. Exception: {}'\
                              .format(data, e))
            return
        self.last_time_saved_monitoring_information = time()
        self.logger.info('Saved monitoring information')
        self.logger.debug('  Information: {}'.format(data))

    def start(self):
        try:
            self.started_at = time()
            self.connect_to_router()
            self.broadcast_subscribe('new job')
            self.get_configuration()
            self._store = self.StoreClass(**self._config['store'])
            self.save_monitoring_information()
            self.run()
        except KeyboardInterrupt:
            self.logger.info('Got SIGNINT (KeyboardInterrupt), exiting.')
            self.worker_pool.end_processes()
            self.worker_pool.kill_processes()
            self.disconnect()

    def start_job(self, job_description):
        worker = job_description['worker']
        worker_requires = self.worker_requirements[worker]
        info = {'worker': worker,
                'worker_requires': worker_requires,
                'data': job_description['data']}
        #TODO: handle if retrieve raises exception
        try:
            worker_input = self._store.retrieve(info)
        except Exception as e:
            #TODO: what to do?
            self.logger.error('Could not retrieve data from store '
                              'with parameters: {}. Exception: {}'\
                              .format(info, e))
            return
        job_info = {'worker': worker,
                    'worker_input': worker_input,
                    'data': job_description['data'],
                    'job id': job_description['job id'],
                    'worker_requires': worker_requires,}
        self.worker_pool.start_job(job_info)
        self.logger.debug('Started job: worker="{}", data="{}"'\
                .format(worker, job_description['data']))

    def get_a_job(self):
        self.logger.debug('Available workers: {}'.format(self.worker_pool.available()))
        for i in range(self.worker_pool.available()):
            self.request({'command': 'get job'})
            message = self.get_reply()
            #TODO: if router stops and doesn't answer, broker will stop here
            if 'worker' in message and message['worker'] is None:
                break # Don't have a job, stop asking
            elif 'worker' in message and 'data' in message:
                if message['worker'] not in self.available_workers:
                    self.logger.info('Ignoring job (inexistent worker): {}'.format(message))
                    #TODO: send a 'rejecting job' request to router
                else:
                    self.start_job(message)
            else:
                self.logger.info('Ignoring malformed job: {}'.format(message))
                #TODO: send a 'rejecting job' request to router

    def router_has_job(self):
        if self.broadcast_poll(self.poll_time):
            message = self.broadcast_receive()
            self.logger.info('[Broadcast] Received from router: {}'\
                             .format(message))
            #TODO: what if broker subscribe to another thing?
            return True
        else:
            return False

    def full_of_jobs(self):
        return len(self.worker_pool) == self.worker_pool.working()

    def should_save_monitoring_information_now(self):
        time_difference = time() - self.last_time_saved_monitoring_information
        return time_difference >= self._config['monitoring interval']

    def check_if_some_job_finished_and_do_what_you_need_to(self):
        for worker in self.worker_pool.workers:
            if not worker.finished_job():
                continue
            job_id = worker.job_info['job id']
            job_data = worker.job_info['data']
            worker_name = worker.job_info['worker']
            worker_requires = worker.job_info['worker_requires']
            start_time = worker.job_info['start time']
            result = worker.get_result()
            end_time = time()
            self.logger.info('Job finished: id={}, worker={}, '
                             'data={}, start time={}, result={}'.format(job_id,
                    worker_name, job_data, start_time, result))

            job_information = {
                    'worker': worker_name,
                    'worker_requires': worker_requires,
                    'worker_result': result,
                    'data': job_data,
            }
            try:
                #TODO: what if I want to the caller to receive job information
                #      as a "return" from a function call? Should use a store?
                #TODO: handle if retrieve raises exception
                try:
                    self._store.save(job_information)
                except Exception as e:
                    #TODO: what to do?
                    self.logger.error('Could not save data into store '
                                      'with parameters: '
                                      '{}. Exception: {}'\
                                      .format(job_information, e))
                    return
            except ValueError:
                self.request({'command': 'job failed',
                              'job id': job_id,
                              'duration': end_time - start_time,
                              'message': "Can't save information on store"})
                #TODO: handle this on router
            else:
                self.request({'command': 'job finished',
                              'job id': job_id,
                              'duration': end_time - start_time})
            result = self.get_reply()
            self.get_a_job()

    def run(self):
        self.get_a_job()
        self.logger.info('Entering main loop')
        while True:
            if self.should_save_monitoring_information_now():
                self.save_monitoring_information()
            if not self.full_of_jobs() and self.router_has_job():
                self.get_a_job()
            self.check_if_some_job_finished_and_do_what_you_need_to()

#TODO: reject jobs if can't get information from store or something like
#      that
