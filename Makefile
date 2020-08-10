SRC=src
TST=test
PYLINT_CONFIG=.pylintrc


.PHONY: all format check-style test doc


all: format check-style test doc
	@echo "⭐ Beginning ALL targets ⭐";

format:
	@echo "⭐ Formatting ⭐";
	black --line-length 120 $(SRC);
	isort -rc $(SRC);
	black --line-length 120 $(TST);
	isort -rc $(TST);

check-style:
	@echo "⭐ Checking Style ⭐"
	prospector 	--strictness medium \
				--max-line-length 120 \
				--with-tool vulture \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(SRC);
	prospector 	--strictness high \
				--max-line-length 120 \
				--with-tool vulture \
				--pylint-config-file $(PYLINT_CONFIG) \
				$(TST);

test:
	@echo "⭐ Performing Tests ⭐"
	coverage run --source=$(SRC) -m pytest test;
	@echo "⭐ Checking Code Coverage ⭐"
	coverage report --fail-under=80;

doc:
	@echo "⭐ Generating Documentation ⭐"
	@echo "doc";
	sphinx-apidoc -f -o docs/source/pages/ $(SRC);
	cd docs && make html;
