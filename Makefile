VIRTUALENV?=virtualenv

.PHONY: clean-pyc clean-build docs

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "lint - check style with flake8"
	@echo "env - create a virtualenv and install requirements"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

lint:
	flake8 txmongoobject tests

env:
	rm -fr env
	mkdir -p .download_cache
	$(VIRTUALENV) --no-site-packages env
	env/bin/pip install --download-cache=.download_cache/ -r requirements.txt

test:
	trial tests

coverage:
	coverage run --source txmongoobject `which trial` tests
	coverage report -m
	coverage html
	open htmlcov/index.html

