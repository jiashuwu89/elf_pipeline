# Science Processing Pipeline

Pipeline to process packets and generate CDFs.

## Relevant Links
- [Auto Documentation](http://science-processing.pages.elfin.ucla/pipeline-refactor/index.html)
- [Coverage Report](http://science-processing.pages.elfin.ucla/pipeline-refactor/htmlcov/index.html)

## General TODOs
- Rename Downlink -> ScienceDownlink
- Standardize processors better
- Increase logging
- Types for all methods
  - https://github.com/pandas-dev/pandas/issues/14468
- Take out all hardcoded times/dates/deltas -> constants.py
- Enums for values
- Increase Testing

## Long Term
- Standardize the passing of pipeline_config/processing_request vs members of these objects
- Style guide
  - Appropriate abbreviations
- Completeness done on the level of science_processor, not idpu_processor + mrm_processor?
- Increase Metrics
  - Timing things with a timing decorator?
- Utilize Prefect
- Use Minio to store State csv's
- Use Docker and Beefboi to run the pipeline

## Using pre-commit
- Add link to hooks directory: `cd .git/hooks && ln -s ../../pre-commit pre-commit`
- Make sure pre-commit file is executable with `chmod`

## Contributors
- Akhil Palla
- Matt Nuesca
- Austin Norris
- Kevin Li
- Wynne Turner
- James King
