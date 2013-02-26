# coding: utf-8

# This example sends all pipelines and then waits for all pipelines to
# finish. It's not a clever way of doing it unless you have a small number
# of pipelines!
# If you have thousands to millions of pipelines to send, you should use
# pipeline_manager.start() and pipeline_manager.update() together in the
# same loop -- and actually you can create a load-balance (send only as
# much pipelines as your cluster can process/finish).

import json
import sys

from time import time

from pypelinin import Job, Pipeline, PipelineManager


def main():
    pipeline_definition = {Job('Downloader'): (Job('GetTextAndWords'),
                                               Job('GetLinks'))}
    urls = ['http://www.fsf.org', 'https://creativecommons.org',
            'http://emap.fgv.br', 'https://twitter.com/turicas',
            'http://www.pypln.org', 'http://www.zeromq.org',
            'http://www.python.org', 'http://www.mongodb.org',
            'http://github.com', 'http://pt.wikipedia.org']

    pipeline_manager = PipelineManager(api='tcp://127.0.0.1:5555',
                                       broadcast='tcp://127.0.0.1:5556')
    print 'Sending pipelines...'
    start_time = time()
    my_pipelines = []
    for index, url in enumerate(urls):
        filename = '/tmp/{}.data'.format(index)
        data = json.dumps({'url': url})
        with open(filename, 'w') as fp:
            fp.write(data)
        pipeline = Pipeline(pipeline_definition, data={'filename': filename})
        pipeline_manager.start(pipeline)
        print '  Sent pipeline for url={}'.format(url)

    print
    print 'Waiting for pipelines to finish...'
    total_pipelines = pipeline_manager.started_pipelines
    finished_pipelines = 0
    while finished_pipelines < total_pipelines:
        pipeline_manager.update(0.5)
        finished_pipelines = pipeline_manager.finished_pipelines
        percentual = 100 * (float(finished_pipelines) / total_pipelines)
        sys.stdout.write('\rFinished pipelines: {}/{} ({:5.2f}%)'\
                         .format(finished_pipelines, total_pipelines,
                                 percentual))
        sys.stdout.flush()
    end_time = time()
    print '\rAll pipelines finished in {} seconds'.format(end_time - start_time)

    durations = [pipeline.duration for pipeline in pipeline_manager.pipelines]
    average_duration = sum(durations) / len(durations)
    print 'Average pipeline duration (seconds) = {} (min={}, max={})'\
          .format(average_duration, min(durations), max(durations))
    print

    print 'Some data saved by store:'
    for index, url in enumerate(urls):
        filename = '/tmp/{}.data'.format(index)
        with open(filename) as fp:
            data = json.loads(fp.read())
        print ('  url={url}, download_duration={download_duration}, '
               'number_of_words={number_of_words}, '
               'number_of_links={number_of_links}'.format(**data))

if __name__ == '__main__':
    main()
