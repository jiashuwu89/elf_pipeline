SRC=src
TST=tst
PYLINT_CONFIG=.pylintrc
LINE_LENGTH=120

# TODO: Add poetry stuff


.PHONY: all format check-style test doc


all: format check-style test doc
	@echo "⭐ Beginning ALL targets ⭐";

format:
	@echo "⭐ Formatting ⭐";
	black --line-length $(LINE_LENGTH) $(SRC);
	isort -rc $(SRC);
	black --line-length $(LINE_LENGTH) $(TST);
	isort -rc $(TST);

# TODO: Vulture broken in prospector right now
# TODO: mypy?
check-style:
	@echo "⭐ Checking Style ⭐"
	prospector 	--strictness medium \
				--max-line-length $(LINE_LENGTH) \
				--with-tool vulture \
				--without-tool pep257 \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(SRC);
	prospector 	--strictness high \
				--max-line-length $(LINE_LENGTH) \
				--with-tool vulture \
				--without-tool pep257 \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(TST);

test:
	@echo "⭐ Performing Tests ⭐"
	coverage run --source=$(SRC) -m pytest test;
	@echo "⭐ Checking Code Coverage ⭐"
	coverage report --fail-under=80;

doc:
	@echo "⭐ Generating Documentation ⭐"
	sphinx-apidoc -f -o doc/source/pages/ $(SRC);
	cd docs && make html;

todo:
	@echo "⭐ Finding TODOs ⭐"
	grep --color=always -Iirn "todo" $(SRC) | sed "s/    //g"

help:
	@echo "⭐ Available Targets ⭐"
	@grep -E "^[a-z\-]+:" Makefile | sed "s/:[ a-z-]*//g"
