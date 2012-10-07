TEST_RUNNER=nosetests -dsv --with-yanc

bootstrap-environment:
	pip install -r requirements/development.txt

bootstrap-tests:
	python pypelinin/setup.py install

test:	bootstrap-tests
	${TEST_RUNNER} tests/

test-manager:	bootstrap-tests
	${TEST_RUNNER} tests/test_manager.py

test-client:	bootstrap-tests
	${TEST_RUNNER} --with-coverage --cover-package=pypelinin.client tests/test_client.py

.PHONY:	bootstrap-environment bootstrap-tests test test-manager
