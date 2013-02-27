# coding: utf-8

import time

from pypelinin import Worker


__all__ = ['Dummy1', 'Dummy2']

class Dummy1(Worker):
    requires = ['']

    def process(self, data):
        return {}

class Dummy2(Worker):
    requires = ['']

    def process(self, data):
        time.sleep(0.01)
        return {}
