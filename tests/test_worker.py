# coding: utf-8

import unittest
import json
from pypelinin import Worker


class WorkerTest(unittest.TestCase):
    def test_pipeline_init(self):
        pipeline = Worker('worker_id')

        self.assertEqual(pipeline.name, 'worker_id')
        self.assertEqual(pipeline.after, [])
        self.assertEqual(pipeline.serialize(), "worker: worker_id")

    def test_pipeline_worker_pipe_pipeline(self):
        pipeline = Worker('w1') | Worker('w2')

        self.assertEqual(pipeline.name, "w1")
        self.assertEqual(pipeline.after, [Worker('w2')])

    def test_pipeline_worker_pipe_parallel_pipelines_pipe_worker(self):
        pipeline = Worker('V1') | [Worker('V2'), Worker('V3')] | Worker('V4')
        self.assertEqual(pipeline.after,
                          [[Worker('V2'), Worker('V3')], Worker('V4')])

    def test_pipeline_worker_pipe_nested_pipe_in_parallel_pipe_worker(self):
        pipeline = Worker('V1') | [ Worker('V2') | Worker('A2'),
                                    Worker('V3')
                                ] | Worker('V4')

        self.assertEqual(pipeline.after,
                          [[Worker('V2') | Worker('A2'),
                                           Worker('V3')], Worker('V4')])

    def test_complex_pipeline_to_json_and_from_json(self):
        pipeline = Worker('V1') | [ Worker('V2') | Worker('A2'),
                                    Worker('V3')
                                ] | Worker('V4')

        jdata = json.dumps(pipeline.to_dict(), indent=4)
        pipeline_from_json = Worker.from_json(jdata)

        self.assertEqual(pipeline, pipeline_from_json)
        self.assertEqual(pipeline_from_json.after,
                          [[Worker('V2') | Worker('A2'),
                                           Worker('V3')], Worker('V4')])
        self.assertEqual(json.dumps(pipeline.to_dict()),
                          json.dumps(pipeline_from_json.to_dict()))
