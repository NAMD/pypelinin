# coding: utf-8

from distutils.core import setup


setup(name='pypelinin',
      version='0.1.0',
      author=u'Álvaro Justen',
      author_email='alvarojusten@gmail.com',
      url='https://github.com/turicas/pypelinin/',
      description='Easily distribute jobs and pipelines among a cluster',
      packages=['pypelinin'],
      install_requires=['pyzmq', 'psutil', 'python-graph-core',
                        'python-graph-dot'],
      license='LGPL',
      keywords=['jobs', 'tasks', 'distributed', 'pipelines', 'cluster'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.7',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Distributed Computing',
      ],
)
