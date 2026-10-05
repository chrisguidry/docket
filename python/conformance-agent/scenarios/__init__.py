"""One module for each scenario, named after it with underscores.

Each module has ``tasks`` for the worker, ``WORKER`` for the worker's
settings, and ``produce(docket)``, which schedules the scenario's tasks.
"""
