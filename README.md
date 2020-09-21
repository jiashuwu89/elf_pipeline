# Science Processing Pipeline

Pipeline to process packets and generate CDFs.

# Relevant Links
[Auto Documentation](http://science-processing.pages.elfin.ucla/pipeline-refactor/index.html)
[Coverage Report](http://science-processing.pages.elfin.ucla/pipeline-refactor/htmlcov/index.html)

## Statuses
- metric
  - Needs testing
- output
  - Needs testing
- processor
  - In progress
- request
  - In progress
- util
  - Grow as necessary

## General TODOs
- Rename Downlink -> ScienceDownlink
- Standardize processors better
- Increase logging
- Types for all variables
  - https://github.com/pandas-dev/pandas/issues/14468
- Favor f-string over string concat, other string creation techniques
- Unify time notation to use only:
  - Datetime (abbreviated dt)
  - Date
- Take out all hardcoded times/dates/deltas -> constants.py
- Enums for values
- Increase Testing

## Tests to write (in order of easiest to hardest)
- byte_tools
- data_type
- output
- metric
- processor
- request
- downlink_utils

# Long Term
- Standardize the passing of pipeline_config/processing_request vs members of these objects
- Style guide
  - Appropriate abbreviations
- Completeness done on the level of science_processor, not idpu_processor + mrm_processor?
- Increase Metrics
  - Timing things with a timing decorator?
- Utilize Prefect

# Using pre-commit
- Add link to hooks directory: `cd .git/hooks && ln -s ../../pre-commit pre-commit`
- Make sure pre-commit file is executable with `chmod`

## Contributors
- Akhil Palla
- Matt Nuesca
- Austin Norris
- Kevin Li
- Wynne Turner
- James King
