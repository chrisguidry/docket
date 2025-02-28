import typer

from docket import __version__

app: typer.Typer = typer.Typer(
    help="Docket - A distributed background task system for Python functions",
    add_completion=True,
    no_args_is_help=True,
)


@app.command(
    help="Start a worker to process tasks",
)
def worker() -> None:
    print("TODO: Configure and start a worker")


@app.command(
    help="Print the version of Docket",
)
def version() -> None:
    print(__version__)
