import typer

from docket import __version__

app = typer.Typer(
    help="Docket - A distributed background task system for Python functions",
    add_completion=True,
    no_args_is_help=True,
)


@app.command(
    help="Start a worker to process tasks",
)
def worker():
    print("TODO: start the worker")


@app.command(
    help="Print the version of Docket",
)
def version():
    print(__version__)
