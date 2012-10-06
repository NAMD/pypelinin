TEST_RUNNER=nosetests -dsv --with-yanc

bootstrap-environment:
	pip install -r requirements/development.txt

bootstrap-tests:
	python pypelinin/setup.py install

test:	bootstrap-tests
	${TEST_RUNNER} tests/

test-manager:	bootstrap-tests
	${TEST_RUNNER} tests/test_manager.py

.PHONY:	bootstrap-environment bootstrap-tests test test-manager
