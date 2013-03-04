pypelinin's Log of Changes
==========================

Version 0.1.1
-------------

Released on 2013-03-04.

This version is basically a bugfix release: it optimizes `PipelineManager`
(actually I consider the behaviour a bug since it's not viable to submit
thousands of pipelines in a reasonable time), fixes the deprecated calls to
some `psutil` functions and removes dependency on
[pygraph](https://code.google.com/p/python-graph/) (since we had
problems with [pyparsing](http://pyparsing.wikispaces.com/) packaging and its
developers didn't help).

The issues that were fixed are:

- [#32](https://github.com/NAMD/pypelinin/issues/32) `PipelineManager.start`
  delays when there are many pipelines being processed -- thanks to
  [@andrebco](https://github.com/andrebco)
- [#33](https://github.com/NAMD/pypelinin/issues/33) Optimize
  `PipelineManager.finished` -- thanks to
  [@andrebco](https://github.com/andrebco)
- [#28](https://github.com/NAMD/pypelinin/issues/28) psutil may not be platform
  independent or may have changed its API -- thanks to
  [@rhcarvalho](https://github.com/rhcarvalho)
- [#42](https://github.com/NAMD/pypelinin/issues/42) Remove pygraph dependency
  -- thanks to [@andrebco](https://github.com/andrebco) (pair programming)
  and [@gvanrossum](https://github.com/gvanrossum) ([`find_cycle`
  implementation](http://neopythonic.blogspot.com/2009/01/detecting-cycles-in-directed-graph.html))
- [#27](https://github.com/NAMD/pypelinin/issues/27) Fix URL in docstring --
  thanks to [@rhcarvalho](https://github.com/rhcarvalho)
- Added a
  [little tutorial to run the example](https://github.com/NAMD/pypelinin/blob/develop/example/index.rst)


Version 0.1.0
-------------

Released on 2012-10-15.

- First version released!
- Have `Router`, `Broker` and `Pipeliner` on "server-side"
- Have `Job`, `Pipeline` and `PipelineManager` on "client-side"
