from typing import (
    Annotated,
)
import logging

from types import (
    SimpleNamespace
)

import typer

import rich
import rich.console
from rich import print
from rich.logging import RichHandler
from rich.traceback import install

from stac_repository import (
    discovered_processors,
    Processor,
)

install(show_locals=True)

logging.basicConfig(
    level="WARNING", format="%(message)s", datefmt="\t", handlers=[RichHandler()]
)


def get_processor(processor_id: str) -> Processor:
    processor: Processor = discovered_processors.get(processor_id)

    if processor is None:
        rich.console.Console(stderr=True).print(f"Processor {processor_id} not found.")
        raise typer.Exit(1)

    return processor


app = typer.Typer(no_args_is_help=True)


@app.callback()
def callback(
    context: typer.Context,
    processor: Annotated[
        str,
        typer.Option(help="Processor id"),
    ] = "stac"
):
    """🌍🛰️\tSTAC Processor

    The interface to process STAC products.
    """
    context.obj = SimpleNamespace(processor=processor)


@app.command()
def show_processors():
    """Shows installed stac-repository processors.
    """

    for processor in discovered_processors.keys():
        print(f" • {processor} version={discovered_processors[processor].__version__}")


@app.command()
def version(
    context: typer.Context,
    product: Annotated[
        str,
        typer.Argument(help="Source product"),
    ]
):
    """Shows some unprocessed source product version.
    """
    processor = get_processor(context.obj.processor)

    print(processor.version(product))


@app.command()
def id(
    context: typer.Context,
    product: Annotated[
        str,
        typer.Argument(help="Source product"),
    ]
):
    """Shows some unprocessed source product id.
    """
    processor = get_processor(context.obj.processor)

    print(processor.id(product))


@app.command()
def process(
    context: typer.Context,
    product: Annotated[
        str,
        typer.Argument(help="Source product"),
    ]
):
    """Processes some source product.
    """
    processor = get_processor(context.obj.processor)

    print(processor.process(product))


@app.command()
def discover(
    context: typer.Context,
    source: Annotated[
        str,
        typer.Argument(help="Source to discover"),
    ]
):
    """Discovers the source products from some source.
    """
    processor = get_processor(context.obj.processor)

    for product in processor.discover(source):
        print(f" • {product}")
