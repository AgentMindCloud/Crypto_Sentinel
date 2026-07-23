.PHONY: install test simulate run build clean

install:
	python -m pip install -e ".[dev]"

test:
	PYTHONPATH=src pytest -q
	python -m compileall -q src

simulate:
	crypto-sentinel --config config.example.yaml simulate

run:
	crypto-sentinel --config config.yaml run

build:
	python -m pip wheel . --no-deps --no-build-isolation -w dist

clean:
	rm -rf build dist .pytest_cache .ruff_cache *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
