SRC=src
TST=tst
PYLINT_CONFIG=.pylintrc
LINE_LENGTH=120
PR=poetry run

# TODO: Add poetry stuff


.PHONY: all format check-style test doc


all: format check-style test doc
	@echo "⭐ Beginning ALL targets ⭐";

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
	$(PR) prospector 	--strictness medium \
				--max-line-length $(LINE_LENGTH) \
				--with-tool vulture \
				--without-tool pep257 \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(SRC) \
	 && prospector 	--strictness high \
				--max-line-length $(LINE_LENGTH) \
				--with-tool vulture \
				--without-tool pep257 \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(TST);

test:
	@echo "⭐ Performing Tests ⭐"
	PYTHONPATH=$(SRC) && $(PR) coverage run --source=$(SRC) -m pytest;
	@echo "⭐ Checking Code Coverage ⭐"
	PYTHONPATH=$(SRC) && $(PR) coverage report --fail-under=80;
	@echo "⭐ Resetting PYTHONPATH"

doc:
	@echo "⭐ Generating Documentation ⭐"
	$(PR) sphinx-apidoc -f -o doc/source/pages/ $(SRC);
	cd doc && make html;

todo:
	@echo "⭐ Finding TODOs ⭐"
	grep --color=always -Iirn "todo" $(SRC) | sed "s/    //g"

help:
	@echo "⭐ Available Targets ⭐"
	@grep -E "^[a-z\-]+:" Makefile | sed "s/:[ a-z-]*//g"
