pypelinin - Example
===================

To run this example you'll need to `install pypelinin
<https://github.com/namd/pypelinin#installation>`_.


Running daemons
---------------

You need to run three daemos (better in different shell sessions)::

    python my_router.py
    python my_pipeliner.py
    python my_broker.py


Running the pipelines
---------------------

With the daemons running, you can submit pipelines. To do it, just run::

    python send_pipelines.py

``send_pipelines.py``'s output will be something like this::

    Sending pipelines...
      Sent pipeline for url=http://www.fsf.org
      Sent pipeline for url=https://creativecommons.org
      Sent pipeline for url=http://emap.fgv.br
      Sent pipeline for url=https://twitter.com/turicas
      Sent pipeline for url=http://www.pypln.org
      Sent pipeline for url=http://www.zeromq.org
      Sent pipeline for url=http://www.python.org
      Sent pipeline for url=http://www.mongodb.org
      Sent pipeline for url=http://github.com
      Sent pipeline for url=http://pt.wikipedia.org

    Waiting for pipelines to finish...
    All pipelines finished in 2.5706679821 seconds
    Average pipeline duration (seconds) = 1.2169262886 (min=0.599352121353, max=2.49300694466)

    Some data saved by store:
      url=http://www.fsf.org, download_duration=0.695417881012, number_of_words=761, number_of_links=69
      url=https://creativecommons.org, download_duration=1.58001089096, number_of_words=870, number_of_links=67
      url=http://emap.fgv.br, download_duration=0.641767024994, number_of_words=314, number_of_links=11
      url=https://twitter.com/turicas, download_duration=2.24012303352, number_of_words=1589, number_of_links=43
      url=http://www.pypln.org, download_duration=1.04617786407, number_of_words=614, number_of_links=22
      url=http://www.zeromq.org, download_duration=0.534137010574, number_of_words=515, number_of_links=15
      url=http://www.python.org, download_duration=1.3195810318, number_of_words=661, number_of_links=64
      url=http://www.mongodb.org, download_duration=0.647953987122, number_of_words=326, number_of_links=44
      url=http://github.com, download_duration=1.17112493515, number_of_words=239, number_of_links=27
      url=http://pt.wikipedia.org, download_duration=1.10309791565, number_of_words=1138, number_of_links=4
