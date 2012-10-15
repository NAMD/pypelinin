# coding: utf-8

import time
import unittest

from textwrap import dedent

from workers import Downloader, GetTextAndWords, GetLinks


sample_html = dedent('''
    <html>
      <head><title>It's a test</title></head>
      <body>
        <a href="http://pypelin.in/">This is a link</a>
        <br />
        <a href="http://www.wikipedia.org/">Another link</a>
        <br />
        <a href="http://www.wikipedia.org/">Another link (repeated)</a>
        <br />
        <a href="http://python.org/">The last one</a>
      </body>
    </html>
''').strip()

class TestDownloader(unittest.TestCase):
    def test_download_local_file(self):
        filename = '/tmp/testing-worker-downloader'
        worker_input = {'url': filename}
        with open(filename, 'w') as fp:
            fp.write(sample_html)
        start_time = time.time()
        result = Downloader().process(worker_input)
        end_time = time.time()
        total_time = end_time - start_time
        self.assertEqual(result['html'], sample_html)
        self.assertEqual(result['length'], len(sample_html))
        self.assertTrue(result['download_duration'] < total_time)

class TestGetTextAndWords(unittest.TestCase):
    def test_simple_html(self):
        worker_input = {'html': sample_html}
        result = GetTextAndWords().process(worker_input)
        expected_text = dedent('''
        It's a test
        This is a link
        Another link
        Another link (repeated)
        The last one
        ''').strip()
        expected_words = expected_text.split()
        self.assertEqual(result['text'], expected_text)
        self.assertEqual(result['words'], expected_words)
        self.assertEqual(result['number_of_words'], len(expected_words))

class TestGetLinks(unittest.TestCase):
    def test_non_repeat_links(self):
        worker_input = {'html': sample_html}
        result = GetLinks().process(worker_input)
        expected_links = ['http://pypelin.in/', 'http://www.wikipedia.org/',
                          'http://python.org/']
        self.assertEqual(set(result['links']), set(expected_links))
        self.assertEqual(result['number_of_links'], len(expected_links))
