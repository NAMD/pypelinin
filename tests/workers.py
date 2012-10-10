# coding: utf-8


import time

__all__ = ['Dummy', 'Echo', 'Upper', 'Snorlax']


class Dummy(object):
    requires = []

    def process(self, data):
        return {}

class Echo(object):
    requires = ['key-a', 'key-b']

    def process(self, data):
        return {'key-c': data['key-a'], 'key-d': data['key-b']}

class Upper(object):
    requires = ['text']

    def process(self, data):
        return {'upper_text': data['text'].upper()}

class Snorlax(object):
    requires = ['sleep-for']

    def process(self, data):
        time.sleep(data['sleep-for'])
        return {}
