# coding: utf-8

import unittest
from pypelinin import Pipeline


class PipelineTest(unittest.TestCase):
    def test_pipeline_init(self):
        pipeline = Pipeline('worker_name')

        self.assertEqual(pipeline._workers, ['worker_name'])
        self.assertEqual(pipeline._after, None)
        self.assertEqual(repr(pipeline), "Pipeline('worker_name')")

    def test_pipeline_with_more_than_one_worker(self):
        pipeline = Pipeline('worker_1', 'worker_2', 'worker_3')

        self.assertEqual(pipeline._workers, ['worker_1', 'worker_2',
                                             'worker_3'])
        self.assertEqual(pipeline._after, None)
        expected = "Pipeline('worker_1', 'worker_2', 'worker_3')"
        self.assertEqual(repr(pipeline), expected)

    def test_pipelines_with_same_workers_should_be_equal(self):
        p1 = Pipeline('w1')
        p2 = Pipeline('w1')
        self.assertEqual(p1, p2)
        p3 = Pipeline('w1', 'w2')
        p4 = Pipeline('w1', 'w2')
        self.assertEqual(p3, p4)

    def test_pipeline_after(self):
        pipeline = Pipeline('w1') | Pipeline('w2')

        self.assertEqual(pipeline._workers, ['w1'])
        self.assertEqual(pipeline._after, Pipeline('w2'))

    def test_pipeline_after_should_not_change_current_object(self):
        p1 = Pipeline('w1')
        p2 = Pipeline('w2')
        p3 = Pipeline('w3.1', 'w3.2')
        p4 = p1 | p2 | p3
        self.assertEqual(p1, Pipeline('w1'))
        self.assertEqual(p2, Pipeline('w2'))
        self.assertEqual(p3, Pipeline('w3.1', 'w3.2'))
        self.assertEqual(p4, p1 | p2 | p3)

    def test_pipeline_after_repr(self):
        self.assertEqual(repr(Pipeline('w1') | Pipeline('w2')),
                         "Pipeline('w1') | Pipeline('w2')")
        self.assertEqual(repr(Pipeline('w1') | Pipeline('w2') | Pipeline('w3')),
                         "Pipeline('w1') | Pipeline('w2') | Pipeline('w3')")

    def test_after_should_raise_TypeError_if_right_object_is_not_Pipeline(self):
        with self.assertRaises(TypeError):
            Pipeline('w1') | 42
        with self.assertRaises(TypeError):
            Pipeline('w1') | 'answer'
        with self.assertRaises(TypeError):
            Pipeline('w1') | 3.14
        with self.assertRaises(TypeError):
            Pipeline('w1') | Exception
        with self.assertRaises(TypeError):
            Pipeline('w1') | Pipeline # class, not instance

    def test_pipelines_with_same_workers_and_after_should_be_equal(self):
        self.assertNotEqual(Pipeline('w1') | Pipeline('w2'), Pipeline('w1'))
        self.assertNotEqual(Pipeline('w1') | Pipeline('w2'), Pipeline('w2'))
        self.assertEqual(Pipeline('w1') | Pipeline('w2'),
                         Pipeline('w1') | Pipeline('w2'))

    def test_pipeline_to_dict_should_serialize_it(self):
        self.assertEqual(Pipeline('w1').to_dict(), {'workers': ['w1'],
                                                    'after': None})
        self.assertEqual(Pipeline('w1', 'w2', 'w3').to_dict(),
                         {'workers': ['w1', 'w2', 'w3'], 'after': None})

        p2 = Pipeline('w1') | Pipeline('w2')
        self.assertEqual(p2.to_dict(), {'workers': ['w1'],
                                        'after': {'workers': ['w2'],
                                                  'after': None}})
        p3 = Pipeline('a', 'b') | Pipeline('c', 'd') | Pipeline('e', 'f', 'g')
        p3_3 = {'workers': ['e', 'f', 'g'], 'after': None}
        p3_2 = {'workers': ['c', 'd'], 'after': p3_3}
        p3_1 = {'workers': ['a', 'b'], 'after': p3_2}
        self.assertEqual(p3.to_dict(), p3_1)

    def test_pipeline_from_dict_should_deserialize_it(self):
        d1 = {'workers': ['w1'], 'after': None}
        self.assertEqual(Pipeline.from_dict(d1), Pipeline('w1'))

        d2 = {'workers': ['w1', 'w2', 'w3'], 'after': None}
        self.assertEqual(Pipeline.from_dict(d2), Pipeline('w1', 'w2', 'w3'))

        d3 = {'workers': ['w1'], 'after': {'workers': ['w2'], 'after': None}}
        self.assertEqual(Pipeline.from_dict(d3),
                         Pipeline('w1') | Pipeline('w2'))

        p3 = Pipeline('a', 'b') | Pipeline('c', 'd') | Pipeline('e', 'f', 'g')
        p3_3 = {'workers': ['e', 'f', 'g'], 'after': None}
        p3_2 = {'workers': ['c', 'd'], 'after': p3_3}
        p3_1 = {'workers': ['a', 'b'], 'after': p3_2}
        self.assertEqual(Pipeline.from_dict(p3_1), p3)
