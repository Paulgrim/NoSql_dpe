SHELL = /bin/bash

.DEFAULT_GOAL := all

## help: Display list of commands
.PHONY: help
help: Makefile
	@sed -n 's|^##||p' $< | column -t -s ':' | sed -e 's|^| |'

## all: Run all targets
.PHONY: all
all: init style run

## init: Bootstrap your application.
.PHONY: init
init:
	pre-commit install -t pre-commit
	pipenv install --dev

## format: Format code.
## style: Check lint, code styling rules.
.PHONY: format
.PHONY: style
style format:
	pre-commit run -a

## clean: Remove temporary files
.PHONY: clean
clean:
	-pipenv --rm

## init-keyspace: Initialise the Cassandra keyspace.
.PHONY: init-keyspace
init-keyspace:
	cqlsh --file=project/init_keyspace.cql

## insert : Insert data in database
.PHONY: insert
insert:
	PYTHONPATH=. pipenv run python project/insert.py

## corr : print correlation between energy consumption and attributes
.PHONY: corr
corr:
	PYTHONPATH=. pipenv run python project/correlation.py

## densite : create csv for density 5km x 5km, 10km x 10km, create geoJson files, print correlation between density and energy consumption
.PHONY: densite
densite:
	PYTHONPATH=. pipenv run python project/densite.py

## econome : study energy consumption by type, print out the most energy-efficient homes, create csv
.PHONY: econome
econome:
	PYTHONPATH=. pipenv run python project/econome.py

## plot: generate pdf files to visualize the distribution curves
.PHONY: plot
plot:
	PYTHONPATH=. pipenv run python project/__main__.py
