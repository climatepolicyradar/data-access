include ./Makefile-vespa.defs

.PHONY: test

test:
	poetry run python -m pytest -vvv

test_not_vespa:
	poetry run python -m pytest -vvv -m "not vespa"
