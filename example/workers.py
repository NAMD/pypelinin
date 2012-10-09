# coding: utf-8

__all__ = ['dummy', 'echo', 'snorlax']


def dummy(document):
    return {}
dummy.__meta__ = {}

def echo(document):
    return {'key-c': document['key-a'], 'key-d': document['key-b']}
echo.__meta__ = {}

def snorlax(document):
    import time
    time.sleep(document['sleep-for'])
    return {}
snorlax.__meta__ = {}
