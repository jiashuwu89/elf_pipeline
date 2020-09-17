SRC=src
TST=tst
PYLINT_CONFIG=.pylintrc
LINE_LENGTH=120
COVERAGE_LIMIT=80
PR=poetry run

# TODO: Add poetry stuff


.PHONY: help all format check-style test coverage doc todo clean


help:
	@echo "⭐ Available Targets ⭐"
	@grep -E "^[a-z\-]+:" Makefile | sed "s/:[ a-z-]*//g"


# TODO: Fix this, a bit of a misnomer

all: format check-style test coverage doc
	@echo "⭐ ALL targets ⭐";

format:
	@echo "⭐ Formatting ⭐";
	$(PR) black --line-length $(LINE_LENGTH) $(SRC) \
	&& isort -rc --lines $(LINE_LENGTH) $(SRC) \
	&& black --line-length $(LINE_LENGTH) $(TST) \
	&& isort -rc --lines $(LINE_LENGTH) $(TST);

# TODO: Vulture broken in prospector right now
# TODO: mypy?
check-style:
	@echo "⭐ Checking Style ⭐"
	$(PR) prospector \
		--strictness medium \
		--max-line-length $(LINE_LENGTH) \
		--with-tool vulture \
		--without-tool pep257 \
		--pylint-config-file $(PYLINT_CONFIG) \
		$(SRC) \
	 && prospector \
	 	--strictness high \
		--max-line-length $(LINE_LENGTH) \
		--with-tool vulture \
		--without-tool pep257 \
		--pylint-config-file $(PYLINT_CONFIG) \
		$(TST);

test:
	@echo "⭐ Performing Tests ⭐"
	PYTHONPATH=$(SRC) && $(PR) coverage run --source=$(SRC) -m pytest;

# --skip-covered can be used to ignore files with 100% coverage
coverage: test
	@echo "⭐ Checking Code Coverage ⭐"
	$(PR) coverage html \
		--precision=2 \
		--fail-under=$(COVERAGE_LIMIT) \
		--skip-empty;

doc:
	@echo "⭐ Generating Documentation ⭐"
	$(PR) sphinx-apidoc -f -o doc/source/pages/ $(SRC);
	cd doc && make html;

todo:
	@echo "⭐ Finding TODOs ⭐"
	grep --color=always -Iirn "todo" $(SRC) | sed "s/    //g"

clean:
	@echo "⭐ Cleaning Files ⭐"
	rm -rf .coverage htmlcov doc/build doc/source/pages
