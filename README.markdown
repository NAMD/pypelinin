pypelinin'
==========

`pypelinin` is a python library to distribute jobs and pipelines among a
cluster. It uses ZeroMQ as its foundation framework for communication between
the daemons.


Architecture
------------

TODO: talk about Router, Broker and Pipeliner


Usage
-----

### Daemons

TODO: talk about starting daemons


### Client

Pypelinin will provide a high level python dsl to describe your workflow.

#### Example 1 - Creating a pipeline

```python
from pypelinin import Pipeline

pipeline = Pipeline('task1') | Pipeline('parallel_1', 'parallel_2') | Pipeline('last_task')
```

#### Example 2 - Submitting a pipeline to be executed

TODO: PipelineManager is not implemented

After defined, you just have to start your pipeline.

```python
from pypelinin import Pipeline, PipelineManager

manager = PipelineManager(api='tcp://localhost:5555',
                          broadcast='tcp://localhost:5556')
pipeline = Pipeline('task1') | Pipeline('task2')
print 'starting executing tasks...'
manager.start(pipeline)
pipeline.wait_finish()
print 'done'
```
