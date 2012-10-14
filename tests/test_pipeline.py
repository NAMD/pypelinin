# coding: utf-8

import unittest
from textwrap import dedent
from pypelinin import Job, Pipeline


class GraphTest(unittest.TestCase):
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
