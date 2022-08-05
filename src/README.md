# Running the Pipeline

There are several ways to run the pipeline:

## Using The `run.py` Entrypoint

The primary way to run the pipeline is by using the Python interpreter to run `run.py`. This is the most direct way to run the pipeline, and is the method used to run the pipeline regularly on the `sciproc-vm`  (via cron).

For additional help, please run `poetry run python run.py --help`.

## Reprocessing Using `reprocess_*.sh`

Additionally, because reprocessing the whole mission's worth of data can be very lengthy, it is typically favorable to process chunks at a time. Thus, it is possible to reprocess/regenerate all files corresponding to a major data product for the entire mission, using the corresponding `reprocess_*.sh`. These scripts chunk the reprocessing by mission and year. If using these scripts, it is highly recommended to run them in a `tmux` session so that detaching is possible.
