# Science Processing Pipeline

Pipeline to process packets and generate CDFs.

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
- Types for all variables
- Favor f-string over string concat, other string creation techniques
- Unify time notation to use only:
  - Datetime (abbreviated dt)
  - Date
- Take out all hardcoded times/dates/deltas -> constants.py
- "types" directory?
  - Enums for values
- Increase Testing

# Long Term
- Standardize the passing of pipeline_config/processing_request vs members of these objects
- Style guide
  - Appropriate abbreviations
- Completeness done on the level of science_processor, not idpu_processor + mrm_processor?
- Increase Metrics
  - Timing things with a timing decorator?
- Utilize Prefect

## Contributors
- Akhil Palla
- Matt Nuesca
- Austin Norris
- Kevin Li
- Wynne Turner
- James King
