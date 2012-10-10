# coding: utf-8

import json

def _to_dict(obj, classkey=None):
    if isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = _to_dict(obj[k], classkey)
        return obj
    elif hasattr(obj, "__iter__"):
        return [_to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, _to_dict(value, classkey))
            for key, value in obj.__dict__.iteritems()
            if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj

class Worker(object):
    def __init__(self, worker_name):
        self.name = worker_name
        self.after = []

    def then(self, *after):
        self.after.extend(list(after))
        return self

    def __or__(self, after):
        self.then(*[after])
        return self

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return "Worker({name})".format(**self.__dict__)

    def serialize(self):
        if not self.after:
            return "worker: {name}".format(name=self.name)
        else:
            data = "main: worker: {name}".format(name=self.name)
            for node in self.after:
                data += " " + node.serialize()
            return data

    @staticmethod
    def from_json(value):
        temp_after = []
        data = json.loads(value)

        if isinstance(data, list):
            for node in data:
                temp_after.append(Worker.from_json(json.dumps(node)))
            return temp_after

        worker = Worker(data['name'])
        worker.after = data['after']
        for node in worker.after:
            temp_after.append(Worker.from_json(json.dumps(node)))

        worker.after = temp_after
        return worker

    def to_dict(self, classkey=None):
        return _to_dict(self, classkey)
