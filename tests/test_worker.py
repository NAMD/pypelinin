# coding: utf-8

import unittest
from pypelinin import Worker


class WorkerTest(unittest.TestCase):
    def test_pipeline_init(self):
        pipeline = Worker('worker_id')

        self.assertEqual(pipeline.name, 'worker_id')
        self.assertEqual(pipeline.after, [])
        self.assertEqual(repr(pipeline), "Worker('worker_id')")

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

    def test_complex_pipeline_to_dict_and_from_dict(self):
        pipeline = Worker('V1') | [ Worker('V2') | Worker('A2'),
                                    Worker('V3')
                                ] | Worker('V4')

        serialized = pipeline.to_dict()
        pipeline_from_dict = Worker.from_dict(serialized)

        self.assertEqual(pipeline, pipeline_from_dict)
        self.assertEqual(pipeline_from_dict.after,
                          [[Worker('V2') | Worker('A2'),
                                           Worker('V3')], Worker('V4')])
        self.assertEqual(pipeline.to_dict(),
                         pipeline_from_dict.to_dict())
