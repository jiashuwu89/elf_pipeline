SRC=src
TST=tst
LINE_LENGTH=120
COVERAGE_LIMIT=80
PR=poetry run


.PHONY: help todo clean format flake8 pylint check-style check-types test coverage doc


help:
	@echo "⭐ Available Targets ⭐"
	@grep -E "^[a-z\-]+:" Makefile | sed "s/:[ ;a-z0-9-]*//g"

todo:
	@echo "⭐ Finding TODOs ⭐"
	grep --color=always -Iirn "todo" $(SRC) | sed "s/    //g";

clean:
	@echo "⭐ Cleaning Files ⭐"
	rm -rf .coverage htmlcov doc/build doc/source/pages

format:
	@echo "⭐ Formatting ⭐";
	$(PR) black --line-length $(LINE_LENGTH) $(SRC);
	$(PR) isort --line-length $(LINE_LENGTH) $(SRC);
	$(PR) black --line-length $(LINE_LENGTH) $(TST);
	$(PR) isort --line-length $(LINE_LENGTH) $(TST);

# E203 (whitespace before ':') conflicts with black, will silence
flake8:
	@echo "⭐ Checking Style with flake8 ⭐";
	$(PR) flake8 $(SRC) --max-line-length $(LINE_LENGTH) --ignore=E203,W503 --benchmark;
	$(PR) flake8 $(TST) --max-line-length $(LINE_LENGTH) --ignore=E203,W503 --benchmark;

pylint:
	@echo "⭐ Checking Style with pylint ⭐";
	$(PR) pylint $(SRC);
	$(PR) pylint $(TST);

check-style: flake8 pylint;

check-types:
	@echo "⭐ Checking Types ⭐"
	$(PR) mypy --ignore-missing-imports $(SRC);

test:
	@echo "⭐ Performing Tests ⭐"
	$(PR) coverage run --source=$(SRC) -m pytest;

# Eliminating the following to use coverage in CI/CD pipeline: --fail-under=$(COVERAGE_LIMIT)
coverage: test
	@echo "⭐ Checking Code Coverage ⭐"
	$(PR) coverage html;
	$(PR) coverage report;

doc:
	@echo "⭐ Generating Documentation ⭐"
	$(PR) sphinx-apidoc -f -o doc/source/pages/ $(SRC);
	cd doc && $(PR) make html;
