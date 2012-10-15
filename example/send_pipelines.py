# coding: utf-8

import json

from pypelinin import Job, Pipeline, PipelineManager


def main():
    pipeline_definition = {Job('Downloader'): (Job('GetTextAndWords'),
                                               Job('GetLinks'))}
    urls = ['http://www.fsf.org', 'https://creativecommons.org',
            'https://github.com', 'http://emap.fgv.br',
            'https://twitter.com/turicas']

    pipeline_manager = PipelineManager(api='tcp://127.0.0.1:5555',
                                       broadcast='tcp://127.0.0.1:5556')
    print 'Sending pipelines...'
    my_pipelines = []
    for index, url in enumerate(urls):
        filename = '/tmp/{}.data'.format(index)
        data = json.dumps({'url': url})
        with open(filename, 'w') as fp:
            fp.write(data)
        pipeline = Pipeline(pipeline_definition, data={'filename': filename})
        pipeline_manager.start(pipeline)
        my_pipelines.append(pipeline)
        print '  Sent pipeline for url={}'.format(url)

    print 'Waiting for pipelines to finish...'
    pipelines_finished = 0
    while pipelines_finished < len(urls):
        counter = 0
        for pipeline in my_pipelines:
            if pipeline_manager.finished(pipeline):
                counter += 1
        if counter != pipelines_finished:
            print ' # of finished pipelines: {}'.format(counter)
            pipelines_finished = counter

    durations = [str(pipeline.duration) for pipeline in my_pipelines]
    print 'Pipeline durations (in seconds) = {}'.format(', '.join(durations))

    for index, url in enumerate(urls):
        filename = '/tmp/{}.data'.format(index)
        with open(filename) as fp:
            data = json.loads(fp.read())
        print ('  url={url}, download_duration={download_duration}, '
               'number_of_words={number_of_words}, '
               'number_of_links={number_of_links}'.format(**data))

if __name__ == '__main__':
    main()
