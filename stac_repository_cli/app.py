from typing import Annotated, Optional
from os import path, PathLike
import io

import pystac
import pystac.layout

import typer

import rich
import rich.console
import rich.status
# import rich.traceback
from rich import print

from stac_repository.managed import MockStacRepositoryManaged
from stac_repository.managed import StacRepositoryManaged
from stac_repository.managed import StacRepositoryConfig

from stac_repository import __version__
from .config import load_config
from .print import print_jobs
from .print import commit_to_rich_str
from .print import list_item

# rich.traceback.install(
#     show_locals=True,
#     word_wrap=True,
#     max_frames=None,
#     locals_max_string=160
# )


def load_repository(
        config_file: PathLike[str],
        mock: bool = False
):
    config = load_config(config_file)

    if mock:
        return MockStacRepositoryManaged(
            path.abspath(config.repository),
            catalog_config=StacRepositoryConfig(
                id=config.catalog.id,
                title=config.catalog.title,
                description=config.catalog.description,
            )
        )
    else:
        return StacRepositoryManaged(
            path.abspath(config.repository),
            catalog_config=StacRepositoryConfig(
                id=config.catalog.id,
                title=config.catalog.title,
                description=config.catalog.description,
            ),
            git_lfs_url=config.git.lfs.url if config.git.lfs else None,
            git_lfs_filter=config.git.lfs.filter if config.git.lfs else None,
            signature=config.git.signature
        )


# rich.traceback refuses to handle __notes__ and ExceptionGroups
app = typer.Typer(pretty_exceptions_enable=False)


@app.callback()
def callback():
    """🌍🛰️\tSTAC Repository

    The interface to manage STAC Repositories.
    """


@app.command()
def version():
    """Show the stac-repository version number.
    """
    print(__version__)


@app.command()
def ingest(
    processor_id: str,
    source: str,
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Discover and ingest products from a source using an installed processor.
    """
    stac_repository = load_repository(config, mock=not git)

    console = rich.get_console()

    print_jobs(
        stac_repository.ingest(
            processor_id,
            source,
        ),
        operation_name="Ingestion [{0}] {1}".format(
            processor_id,
            source
        ),
        console=console
    )


@app.command()
def discover(
    processor_id: str,
    source: str,
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Discover products from a source using an installed processor.
    """
    stac_repository = load_repository(config, mock=not git)

    console = rich.get_console()

    console.print(f"[bold]{processor_id} : {source}[/bold]")
    for product_source in stac_repository.discover(processor_id, source):
        console.print(list_item(product_source))


@app.command()
def ingest_products(
    processor_id: str,
    *product_sources: list[str],
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Discover products from a source using an installed processor.
    """
    stac_repository = load_repository(config, mock=not git)

    console = rich.get_console()

    print_jobs(
        stac_repository.ingest_products(processor_id, *product_sources),
        operation_name="Ingestion [{0}]".format(
            processor_id
        ),
        console=console
    )


@app.command()
def prune(
    product_ids: list[str],
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Delete products from the catalog.
    """
    stac_repository = load_repository(config, mock=not git)

    console = rich.get_console()

    print_jobs(
        stac_repository.prune(*product_ids),
        operation_name="Deletion",
        console=console
    )


@app.command()
def history(
    product_id: Annotated[Optional[str], typer.Argument()] = None,
    verbose: bool = False,
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Display the catalog history.

    If --product_id is specified, then limit the output to commits where this product existed in the catalog
    """
    stac_repository = load_repository(config, mock=not git)

    console = rich.get_console()

    if product_id:
        console.print(f"[bold]Product History {product_id}[/bold]")
    else:
        console.print(f"[bold]History[/bold]")

    for commit in stac_repository.history(product_id):
        console.print(list_item(commit_to_rich_str(
            commit,
            include_message=verbose
        )))


@app.command()
def rollback(
    ref: str,
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Rollback the catalog to a previous commit.

    --ref must be a commit ref.

    Datetime rollback are not supported yet.
    """
    stac_repository = load_repository(config, mock=not git)
    stac_repository.rollback(ref)


@app.command()
def backup(
    url: str,
    config: str = "stac_repository.toml",
    git: bool = True,
):
    """Clone (or pull) the repository **to** a backup location.

    ssh:// backups are not supported yet.
    """
    stac_repository = load_repository(config, mock=not git)
    stac_repository.backup(url)
