# coding: utf-8

class Pipeline(object):
    '''Representation of a series of parallel workers

    A `Pipeline` is basically a list of workers that will execute in parallel.
    For example:
        >>> my_pipeline = Pipeline('w1', 'w2')

    This represents that worker 'w1' will run in parallel to worker 'w2'.
    You can combine pipelines using the `|` operator, as follows:
        my_pipeline = Pipeline('s1') | Pipeline('s2')
    In the example above, worker 's1' will be executed and then, sequentially,
    's2'.
    The `|` operation returns a new `Pipeline` object, so you can combine your
    pipeline objects easily as:
        >>> p1 = Pipeline('w1.1', 'w1.2')
        >>> p2 = Pipeline('s2.1') | Pipeline('s2.2')
        >>> p3 = p1 | p2
        >>> print repr(p3)
        Pipeline('w1.1', 'w1.2') | Pipeline('s2.1') | Pipeline('s2.2')
    '''
    def __init__(self, *workers):
        self._workers = []
        for worker in workers:
            self._workers.append(worker)
        self._after = None

    def __repr__(self):
        workers_representation = [repr(w) for w in self._workers]
        if self._after is None:
            return 'Pipeline({})'.format(', '.join(workers_representation))
        else:
            workers = ', '.join(workers_representation)
            return 'Pipeline({}) | {}'.format(workers, repr(self._after))

    def __or__(self, other):
        if type(self) != type(other):
            raise TypeError('You can only use "|" between Pipeline objects')
        p = Pipeline(*self._workers)
        if self._after is None:
            p._after = other
        else:
            p._after = self._after | other
        return p

    def __eq__(self, other):
        return type(self) == type(other) and \
               self._workers == other._workers and \
               self._after == other._after

    def to_dict(self):
        after = None
        if self._after is not None:
            after = self._after.to_dict()
        return {'workers': self._workers, 'after': after}

    @staticmethod
    def from_dict(data):
        p = Pipeline(*data['workers'])
        if data['after'] is not None:
            p = p | Pipeline.from_dict(data['after'])
        return p
