#!/usr/bin/env python
# coding: utf-8

import os
import sys

from time import time

import psutil

from pypelinin import PipelineManager, Pipeline, Job


ROUTER_API = 'tcp://localhost:12345'
ROUTER_BROADCAST = 'tcp://localhost:12346'
NUMBER_OF_PIPELINES = 30000
UPDATE_INTERVAL = 100

def main():
    stdout_write = sys.stdout.write
    stdout_flush = sys.stdout.flush
    pipeline_manager = PipelineManager(api=ROUTER_API,
                                       broadcast=ROUTER_BROADCAST)
    pipeline_definition = {Job('Dummy1'): Job('Dummy2')}
    process = psutil.Process(os.getpid())
    version = sys.argv[1]
    filename = 'test-{}_pipelines-pypelinin-{}.dat'.format(NUMBER_OF_PIPELINES,
                                                           version)
    data = open(filename, 'w')
    my_pipelines = []
    for i in xrange(NUMBER_OF_PIPELINES):
        pipeline = Pipeline(pipeline_definition, data={'index': i})
        start_time = time()
        pipeline_manager.start(pipeline)
        end_time = time()
        my_pipelines.append(pipeline)
        memory_info = process.get_memory_info()
        info = (i + 1, end_time - start_time, memory_info.vms, memory_info.rss)
        data.write('{}\t{}\t{}\t{}\n'.format(*info))
        if (i + 1) % UPDATE_INTERVAL == 0:
            stdout_write('\r{} out of {}'.format(i + 1, NUMBER_OF_PIPELINES))
            stdout_flush()
    stdout_write('\rfinished sending pipelines! \o/\n')

    stdout_write('Waiting for pipelines to finish...\n')
    pipelines_finished = 0
    finished = pipeline_manager.finished
    while pipelines_finished < NUMBER_OF_PIPELINES:
        finished(my_pipelines[0]) # just need one call to update state of all
        counter = [pipeline.finished for pipeline in my_pipelines].count(True)
        if counter != pipelines_finished:
            stdout_write('\r # of finished pipelines: {}/{}'.format(counter,
                    NUMBER_OF_PIPELINES))
            stdout_flush()
            pipelines_finished = counter
    stdout_write('\n')
    data.close()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'ERROR: you must specify a version number.'
        exit(1)
    main()
