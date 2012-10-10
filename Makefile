TEST_RUNNER=nosetests -dsv --with-yanc

clean:
	find -regex '.*\.pyc' -exec rm {} \;
	find -regex '.*~' -exec rm {} \;

bootstrap-environment:
	pip install -r requirements/development.txt

bootstrap-tests: clean
	clear
	python pypelinin/setup.py install

test:	bootstrap-tests
	${TEST_RUNNER} tests/

test-router:	bootstrap-tests
	${TEST_RUNNER} tests/test_router.py

test-client:	bootstrap-tests
	${TEST_RUNNER} --with-coverage --cover-package=pypelinin.client tests/test_client.py

test-broker:	bootstrap-tests
	${TEST_RUNNER} -x tests/test_broker.py

.PHONY:	clean bootstrap-environment bootstrap-tests test test-router test-client test-broker
