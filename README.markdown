pypelinin'
==========

`pypelinin` is a python library to distribute pipelines (collections of jobs)
among a cluster. It uses ZeroMQ as its foundation framework for communication
between the daemons.

You need to provide code for the workers, the store (what will retrieve and
save information to/from workers work), start the daemons in as many machines
as you want and then submit pipelines to be processed.

[![pypelinin's build status on travis-ci.org](https://travis-ci.org/NAMD/pypelinin.png)](https://travis-ci.org/NAMD/pypelinin)

Architecture
------------

`pypelinin` has 3 daemons you need to run:

- **Router**: the central point of communication of the network. When you need
  to execute a pipeline, all you have to do is ask the Router to add it, and
  every other daemon will communicate with the Router to get a pipeline for
  execution. You can have only one Router running.
- **Broker**: runs worker processes and executes jobs. It does not know about
  an entire pipeline, it just receives a job to be executed, retrieves needed
  information for that job, runs the worker and then saves information returned
  by the worker. It uses a class defined by you (`StoreClass`) to retrieve/save
  information. You should run as many Brokers as possible in your cluster, to
  increase throughtput of job/pipeline execution.
- **Pipeliner**: takes care of pipelines. This daemon do not know how to
  save/retrieve or even execute jobs, but it knows which job should be executed
  after another one in a pipeline. Router will give Pipeliner a pipeline and it
  will ask for job execution (to Router, that will be sent to Broker). You can
  run as many Pipeliners as you want (but one is enough to handle lots of
  pipelines simultaneously).

A image is better than 1,000 words:

![pypelinin's architecture](https://f.cloud.github.com/assets/186126/203314/51bbb686-8132-11e2-88df-3a455b71d89a.png)


Installation
------------

First you need to install `libzmq`, its headers and compilers needed to compile
it. On a Debian/Ubuntu machine, run:

    sudo aptitude install libzmq1 libzmq-dev build-essential

Then, install the Python package:

    pip install pypelinin


Usage
-----

### Daemons

For each daemon, you need to create a script that instantiates the daemon class
and starts it. Please check our
[example](https://github.com/turicas/pypelinin/tree/develop/example)
(files `example/my_router.py`, `example/my_broker.py` and
`example/my_pipeliner.py`).


### Client

You need to specify what jobs are in a pipeline and then send it to Router.
A pipeline is a
[directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph)
(aka DAG) and is represented as a `dict`, where key/value pairs represent edges
(keys are "from" and values are "to" edges --
[see notes about this representation](http://www.python.org/doc/essays/graphs/)).


#### Example Creating a pipeline and submitting it to execution

```python
from pypelinin import Pipeline, Job, PipelineManager


pipeline = Pipeline({Job('WorkerName1'): Job('WorkerName2'),
                     Job('WorkerName2'): Job('WorkerName3')},
                    data={'foo': 'bar'})
```

In this pipeline, `Job('WorkernName2')` will be executed after
`Job('WorkerName1')` and `Job('WorkerName3')` after `Job('WorkerName2')` --
when you send it to `Pipeliner` (via `Router`), it'll take care of executing
the jobs in this order. `data` is what will be passed to `StoreClass` (that is
loaded on each `Broker`) when `Broker` needs to retrieve information from a
data store to pass it to a worker execute or to save information returned by
the worker.

You can also generate a [DOT file]() from the pipeline object, so you can then
plot it to represent the entire pipeline in a presentation or report:

```python
print str(pipeline)
```

And the result:

```dot
digraph graphname {
    "WorkerName2";
    "WorkerName3";
    "WorkerName1";

    "WorkerName2" -> "WorkerName3";
    "WorkerName1" -> "WorkerName2";
}
```

Or simply:

```python
pipeline.save_dot('filename.dot')
```

Then you can run [graphviz](http://graphviz.org/) to generate a PNG, for example:

```bash
dot -Tpng -omy_pipeline.png filename.dot
```

As soon the pipeline object is created, we need to connect to our `Router` and
submit it to execution:

```python

manager = PipelineManager(api='tcp://localhost:5555',
                          broadcast='tcp://localhost:5556')
manager.start(pipeline) # send it to the cluster to execute
while not pipeline.finished: # wait for pipeline to finish
    manager.update()
print 'done'
```

Note that you need to create a `StoreClass` and the workers (each one is a
another class). These classes should be passed to a `Broker` when instantiated
(see more details below in the tutorial or in our
[example](https://github.com/turicas/pypelinin/tree/develop/example)).


Tutorial
--------

Let's learn doing! Create a virtualenv, install pypelinin and then download our
`example` folder to see it working.

    mkvirtualenv test-pypelinin
    pip install pypelinin
    wget https://github.com/turicas/pypelinin/tarball/develop -O pypelinin.tar.gz
    tar xfz pypelinin.tar.gz && rm pypelinin.tar.gz
    cd turicas-pypelinin-*/example/

Now your environment is created and you need to run the daemons, each one in a
separated terminal:

Router:

    $ python my_router.py
    2012-10-15 14:12:59,112 - My Router - INFO - Entering main loop

Broker:

    $ python my_broker.py
    2012-10-15 14:13:17,956 - Broker - INFO - Starting worker processes
    2012-10-15 14:13:18,055 - Broker - INFO - Broker started
    2012-10-15 14:13:18,056 - Broker - INFO - Trying to connect to router...
    2012-10-15 14:13:18,057 - Broker - INFO - [API] Request to router: {'command': 'get configuration'}
    2012-10-15 14:13:18,058 - Broker - INFO - [API] Reply from router: {u'monitoring interval': 60, u'store': {u'monitoring filename': u'/tmp/monitoring.log'}}

And Pipeliner:

    $ python my_pipeliner.py
    2012-10-15 14:13:56,476 - Pipeliner - INFO - Pipeliner started
    2012-10-15 14:13:56,477 - Pipeliner - INFO - Entering main loop
    2012-10-15 14:13:56,477 - Pipeliner - INFO - Bad bad router, no pipeline for me.

Please read the files:
- `file\_store.py` - we have a simple StoreClass which saves and retrieves
  information from files. You can modify it easily to use a database.
- `workers.py` (and `test\_workers.py`) - we have created 3 workers:
  `Downloader`, `GetTextAndWords` and `GetLinks`. The first one is required to
  execute the last two. Each worker is basically a class that inherites from
  `pypelinin.Worker`, have an attribute `requires` and a method `process`.
- `send\_pipelines.py` - this script basically creates some `Pipeline`s and
  send it to execution using a `PipelineManager` (as the example above). You
  need to run it to get the jobs executed.

After executing `send\_pipelines.py` you can check files
`/tmp/{0,1,2,3,4}.data` to see the results -- these files are python
dictionaries encoded as JSON (this was done by `file\_store.SimpleFileStore`).
To read one of these files, just call this function:

```python
import json

def read_result_file(filename):
    with open(filename, 'r') as fp:
        data = fp.read()
    return json.loads(data)
```

### Installing on other cluster nodes

If you want to process more jobs/pipelines per second, you need to run more
Brokers on another machines. To do it, you need to:

- Be sure `Router` is binding to an interface that is reachable to all machines
  that will run `Broker` and `Pipeline` (change `my\_router.py`);
- Change `my\_broker.py` with new `Router` ip address/ports;
- Install `pypelinin` in all cluster machines;
- Copy `my\_broker.py`, `file\_store.py` and `workers.py` to all
  "Broker machines";
- Run everything!


Related software
----------------

- [Resque](https://github.com/defunkt/resque)
- [Powerhose](http://powerhose.readthedocs.org/)
- [Circus](http://circus.readthedocs.org/)
- [Sentry](http://pypi.python.org/pypi/sentry/)
- [Celery](http://www.celeryproject.org/)
- [Compmake](http://andreacensi.github.com/compmake/)
