pypelinin
=========

Python library to distribute jobs and pipelines among a cluster.

Usage
=====

Pypelinin will provide a high level python dsl to describe your workflow.

Example 1:

```python
pipeline = Worker('do a task') | [Worker('parallel task 1'),
                                  Worker('parallel task 2')] | Worker('finalizer')
```

Example 2:

```python
pipeline = Worker('do a task') | [Worker('parallel task 1') | Worker('after 1'),
                                  Worker('parallel task 2')] | Worker('finalizer')
```

After defined, you just have to start your pipeline.

```
Pipeliner.start(pipeline)
```
