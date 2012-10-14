# coding: utf-8

import unittest
from textwrap import dedent
from pypelinin import Job, Pipeline

class JobTest(unittest.TestCase):
    def test_worker_name(self):
        self.assertEqual(Job('ABC').worker_name, 'ABC')

    def test_should_start_with_no_data(self):
        self.assertEqual(Job('ABC').data, None)

    def test_repr(self):
        self.assertEqual(repr(Job('ABC')), "Job('ABC')")

    def test_equal_not_equal_and_hash(self):
        job_1 = Job('qwe')
        job_2 = Job('qwe')
        job_3 = Job('bla')
        self.assertTrue(job_1 == job_2)
        self.assertTrue(job_2 == job_1)
        self.assertTrue(job_1 != job_3)
        self.assertTrue(job_3 != job_1)
        self.assertEqual(hash(job_1), hash(job_2))
        self.assertNotEqual(hash(job_1), hash(job_3))

class PipelineTest(unittest.TestCase):
    def test_jobs(self):
        result = Pipeline({Job('A'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')],
                           Job('W'): Job('A')}).jobs
        expected = (Job('A'), Job('B'), Job('C'), Job('D'), Job('E'), Job('W'),
                    Job('Z'))
        self.assertEqual(set(result), set(expected))

    def test_get_starters(self):
        result = Pipeline({Job('A'): []}).starters
        expected = (Job('A'),)
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [], Job('B'): []}).starters
        expected = (Job('A'), Job('B'))
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')], Job('B'): []}).starters
        expected = (Job('A'),)
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')]}).starters
        expected = (Job('A'), Job('Z'))
        self.assertEqual(set(result), set(expected))

        result = Pipeline({('A', 'B', 'C'): ['D']}).starters
        expected = ['A', 'B', 'C']
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                           Job('E'): (Job('B'), Job('F'))}).starters
        expected = (Job('A'), Job('C'), Job('E'))
        self.assertEqual(set(result), set(expected))

    def test_normalize(self):
        result = Pipeline({Job('A'): Job('B')})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')]})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'),): (Job('B'),)})._graph
        expected = [(Job('A'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('C')): Job('B')})._graph
        expected = [(Job('A'), Job('B')), (Job('C'), Job('B'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({('A', 'C'): ['B', 'D', 'E']})._graph
        expected = [('A', 'B'), ('A', 'D'), ('A', 'E'), ('C', 'B'), ('C', 'D'),
                    ('C', 'E')]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('ABC'): []})._graph # problem here if use string
        expected = [(Job('ABC'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [], Job('B'): []})._graph
        expected = [(Job('A'), None), (Job('B'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('A'): [Job('B')], Job('B'): []})._graph
        expected = [(Job('A'), Job('B')), (Job('B'), None)]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({Job('QWE'): [Job('B')],
                           Job('B'): [Job('C'), Job('D'), Job('E')],
                           Job('Z'): [Job('W')]})._graph
        expected = [(Job('QWE'), Job('B')), (Job('B'), Job('C')),
                    (Job('B'), Job('D')), (Job('B'), Job('E')),
                    (Job('Z'), Job('W'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')]})._graph
        expected = [(Job('A'), Job('D')), (Job('B'), Job('D')),
                    (Job('C'), Job('D'))]
        self.assertEqual(set(result), set(expected))

        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                           Job('E'): (Job('B'), Job('F'))})._graph
        expected = [(Job('A'), Job('D')), (Job('B'), Job('D')),
                    (Job('C'), Job('D')), (Job('E'), Job('B')),
                    (Job('E'), Job('F'))]
        self.assertEqual(set(result), set(expected))

    def test_validate_graph(self):
        #should have at least one starter node
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): Job('A')})
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): [Job('B')], Job('B'): [Job('A')]})

        #should not have cycles
        with self.assertRaises(ValueError):
            print Pipeline({Job('A'): [Job('B')], Job('B'): [Job('C')],
                      Job('C'): [Job('B')]})._graph
        with self.assertRaises(ValueError):
            Pipeline({Job('A'): [Job('B')], Job('B'): [Job('C')],
                      Job('C'): [Job('D')], Job('D'): [Job('B')]})

    def test_dot(self):
        result = Pipeline({(Job('A'), Job('B'), Job('C')): [Job('D')],
                           Job('E'): (Job('B'), Job('F'))}).to_dot().strip()
        expected = dedent('''
        digraph graphname {
        "Job('A')";
        "Job('C')";
        "Job('B')";
        "Job('E')";
        "Job('D')";
        "Job('F')";
        "Job('A')" -> "Job('D')";
        "Job('C')" -> "Job('D')";
        "Job('B')" -> "Job('D')";
        "Job('E')" -> "Job('B')";
        "Job('E')" -> "Job('F')";
        }
        ''').strip()

        self.assertEqual(result, expected)

    def test_pipeline_should_propagate_data_among_jobs(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: job_2, job_2: job_3}, data=pipeline_data)
        self.assertEqual(pipeline.data, pipeline_data)
        self.assertEqual(job_1.data, pipeline_data)
        self.assertEqual(job_2.data, pipeline_data)
        self.assertEqual(job_3.data, pipeline_data)
        self.assertEqual(job_1.pipeline, pipeline)
        self.assertEqual(job_2.pipeline, pipeline)
        self.assertEqual(job_3.pipeline, pipeline)

    def test_pipeline_add_finished_job(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: job_2, job_2: job_3}, data=pipeline_data)
        job_4 = Job('w4')

        self.assertFalse(pipeline.finished_job(job_1))
        self.assertFalse(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_1)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertFalse(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_2)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertTrue(pipeline.finished_job(job_2))
        self.assertFalse(pipeline.finished_job(job_3))

        pipeline.add_finished_job(job_3)
        self.assertTrue(pipeline.finished_job(job_1))
        self.assertTrue(pipeline.finished_job(job_2))
        self.assertTrue(pipeline.finished_job(job_3))

        with self.assertRaises(ValueError):
            pipeline.add_finished_job(job_4) # job not in pipeline
        with self.assertRaises(RuntimeError):
            pipeline.add_finished_job(job_3) # already finished

    def test_pipeline_finished(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: job_2, job_2: job_3}, data=pipeline_data)

        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_1)
        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_2)
        self.assertFalse(pipeline.finished())
        pipeline.add_finished_job(job_3)
        self.assertTrue(pipeline.finished())

    def test_default_attributes(self):
        pipeline = Pipeline({Job('test'): None})
        self.assertEqual(pipeline.data, None)
        self.assertEqual(pipeline.jobs, (Job('test'),))
        self.assertEqual(pipeline.sent_jobs, set())

    def test_available_jobs(self):
        job_1 = Job('w1')
        job_2 = Job('w2')
        job_3 = Job('w3')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: job_2, job_2: job_3}, data=pipeline_data)

        expected = [job_1]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_1)
        expected = [job_2]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_2)
        expected = [job_3]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_3)
        self.assertEqual(pipeline.available_jobs(), set())

        job_4, job_5, job_6, job_7 = Job('w4'), Job('w5'), Job('w6'), Job('w7')
        job_8, job_9, job_10 = Job('8'), Job('9'), Job('10')
        job_11, job_12, job_13 = Job('11'), Job('12'), Job('13')
        job_14, job_15, job_16 = Job('14'), Job('15'), Job('16')
        pipeline_data = {'python': 42}
        pipeline = Pipeline({job_1: (job_2, job_3),
                             job_2: (job_4, job_16),
                             job_3: job_4,
                             job_4: job_5,
                             job_5: (job_6, job_7, job_8, job_9),
                             (job_6, job_7, job_8): job_10,
                             (job_10, job_11): (job_12, job_13, job_14),
                             job_15: None},
                            data=pipeline_data)

        expected = [job_1, job_11, job_15]
        self.assertEqual(pipeline.available_jobs(), set(expected))
        self.assertEqual(pipeline.available_jobs(), set(pipeline.starters))

        pipeline.add_finished_job(job_1)
        expected = [job_11, job_15, job_2, job_3]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_2)
        expected = [job_11, job_15, job_3, job_16]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_3)
        expected = [job_11, job_15, job_4, job_16]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_16)
        expected = [job_11, job_15, job_4]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_4)
        expected = [job_11, job_15, job_5]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_11)
        expected = [job_15, job_5]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_5)
        expected = [job_15, job_6, job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_6)
        expected = [job_15, job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_15)
        expected = [job_7, job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_7)
        expected = [job_8, job_9]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_9)
        expected = [job_8]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_8)
        expected = [job_10]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_10)
        expected = [job_12, job_13, job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_13)
        expected = [job_12, job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_12)
        expected = [job_14]
        self.assertEqual(pipeline.available_jobs(), set(expected))

        pipeline.add_finished_job(job_14)
        expected = []
        self.assertEqual(pipeline.available_jobs(), set(expected))

        self.assertTrue(pipeline.finished())
