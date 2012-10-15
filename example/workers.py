# coding: utf-8


import re
import time
import urllib2

from pypelinin import Worker


__all__ = ['Downloader', 'GetTextAndWords', 'GetLinks']

class Downloader(Worker):
    requires = ['url']

    def process(self, data):
        url = data['url']
        start_time = time.time()
        opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        response = opener.open(url)
        content = response.read()
        response.close()
        end_time = time.time()
        total_time = end_time - start_time
        return {'html': content, 'download_duration': total_time,
                'length': len(content)}

regexp_tags = re.compile(r'<[^>]*>')
regexp_spaces = re.compile(r'\n[ ]*')

class GetTextAndWords(Worker):
    requires = ['html']

    def process(self, data):
        html = data['html']
        without_tags = regexp_tags.sub('', html)
        text = regexp_spaces.sub('\n', without_tags).strip()
        while '\n\n' in text:
            text = text.replace('\n\n', '\n')
        words = text.split()
        return {'text': text, 'words': words, 'number_of_words': len(words)}

regexp_links = re.compile(r'http://([^ "]*)')

class GetLinks(Worker):
    requires = ['html']

    def process(self, data):
        html = data['html']
        links = set(regexp_links.findall(html)) # do not repeat links
        links = ['http://' + link for link in links]
        return {'links': links, 'number_of_links': len(links)}
