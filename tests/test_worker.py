import unittest

import json
from pypelinin.worker import Worker, todict

class WorkerTest(unittest.TestCase):
    def test_pipeline_init(self):
        pipeline = Worker('worker_id')

        self.assertEquals(pipeline.name, 'worker_id')
        self.assertEquals(pipeline.after, [])
        self.assertEquals(pipeline.serialize(), "worker: worker_id")

    def test_pipeline_worker_pipe_pipeline(self):
        pipeline = Worker('w1') | Worker('w2')

        self.assertEquals(pipeline.name, "w1")
        self.assertEquals(pipeline.after, [Worker('w2')])

    def test_pipeline_worker_pipe_parallel_pipelines_pipe_worker(self):
        pipeline = Worker('V1') | [Worker('V2'), Worker('V3')] | Worker('V4')
        self.assertEquals(pipeline.after,
                          [[Worker('V2'), Worker('V3')], Worker('V4')])

    def test_pipeline_worker_pipe_nested_pipe_in_parallel_pipe_worker(self):
        pipeline = Worker('V1') | [ Worker('V2') | Worker('A2'),
                                    Worker('V3')
                                ] | Worker('V4')

        self.assertEquals(pipeline.after,
                          [[Worker('V2') | Worker('A2'),
                                           Worker('V3')], Worker('V4')])

    def test_complex_pipeline_to_json_and_from_json(self):
        pipeline = Worker('V1') | [ Worker('V2') | Worker('A2'),
                                    Worker('V3')
                                ] | Worker('V4')

        jdata = json.dumps(todict(pipeline), indent=4)
        pipeline_from_json = Worker.from_json(jdata)

        self.assertEquals(pipeline, pipeline_from_json)
        self.assertEquals(pipeline_from_json.after,
                          [[Worker('V2') | Worker('A2'),
                                           Worker('V3')], Worker('V4')])
        self.assertEquals(json.dumps(todict(pipeline)),
                          json.dumps(todict(pipeline_from_json)))

