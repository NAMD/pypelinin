PipelineManager.start benchmark
===============================

This benchmark is related to issue
[#33](https://github.com/NAMD/pypelinin/issues/33).

We found out that `PipelineManager.start` method was taking too much time to
run when we submitted lots of pipelines in the same `PipelineManager` instance.
So, this benchmark was created to check memory and CPU usage of the current
implementation and to suggest/implement (and run the same tests on) a new one.


What was tested
---------------

A `send_pipelines.py` script was created to send 30,000 pipelines with dummy
workers. This script recorded how much memory each new pipeline added to its
process and how much time `PipelineManager.start` took to run -- we found out
that the time taken by `PipelineManager.start` method was increasing linearly
with the number of submitted pipelines.

By inspecting the code, an unnecessary `list` object was found and replaced by
an implementation using a `dict`, that solved the time problem. As the `list`
was removed, the memory footprint of the new version was also reduced.


Running the benchmark
---------------------

Before running the benchmark you need to boostrap the environment:

    cd benchmarks/optimize-PipelineManager
    ./bootstrap.sh

Then, you'll need two terminal sessions.

## Test version 0.1.0

In one terminal session, run:

    ./run-daemons.sh 0.1.0

And in the other one:

    ./run-benchmark.sh 0.1.0

After `run-benchmark.sh` finishes running, you should go to the terminal
`run-benchmarks.sh` is running and press `Ctrl-C`.


## Test version 'fixed'

In one terminal session, run:

    ./run-daemons.sh fixed

And in the other one:

    ./run-benchmark.sh fixed

After `run-benchmark.sh` finishes running, you should go to the terminal
`run-benchmarks.sh` is running and press `Ctrl-C`.


## Plotting graphs

To plot graphs you need to have [gnuplot](http://www.gnuplot.info/) installed.
Then, run:

    ./plot.sh

The files `time.png` and `memory.png` will be created (you can see them below).


Results
-------

## `PipelineManager.start` call duration

The call duration was increasing linearly with the number of pipelines. Now,
it's a fixed number, independent of the number of pipelines submitted.

![time](https://f.cloud.github.com/assets/186126/199948/60584cdc-80af-11e2-912f-43a3369f53a7.png)

## Memory footprint

The memory footprint was reduced since we are not duplicating pipelines'
content (in `list` **and** `dict`) anymore -- now we're using only a `dict`.
However, we still needs to improve this memory usage, since `PipelineManager`
will **always** increase memory usage even when pipelines are finished (we're
working on this problem on issue
[#39](https://github.com/NAMD/pypelinin/issues/39)).

![memory](https://f.cloud.github.com/assets/186126/199949/6715b19a-80af-11e2-960a-4a657771c92d.png)
