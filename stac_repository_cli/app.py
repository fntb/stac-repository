from typing import (
    Optional,
    List,
    Tuple,
    cast
)

import sys

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated


from types import (
    SimpleNamespace
)

import logging
import os

import typer

from rich import print
from rich import prompt
from rich.logging import RichHandler
from rich.traceback import install

from pydantic import (
    ValidationError,
    BaseModel
)

from stac_repository import (
    __version__,
    discovered_processors,
    BaseStacRepository,
    Backend,
    Catalog,
    RepositoryNotFoundError,
    CommitNotFoundError,
    BackupValueError,
    RefTypeError,
    ProcessorNotFoundError,
    ProcessingError,
    ErrorGroup,
    StacObjectError,
    HrefError,
    CatalogError,
    UncatalogError,
)

from stac_repository_cli.backends import discovered_backends

from .config import RepositoriesConfig

from .print import (
    print_reports,
    print_error,
    print_list,
    style_list_item,
    style_commit,
    style_bold
)

install(show_locals=True)

logging.basicConfig(
    level="WARNING", format="%(message)s", datefmt="\t", handlers=[RichHandler()]
)


class BackendNotFoundError(ValueError):
    pass

# def prompt_catalog() -> Catalog:
#     id = prompt.Prompt.ask("id", default="root")
#     title = prompt.Prompt.ask("title")
#     description = prompt.Prompt.ask("description")
#     license = prompt.Prompt.ask("license", default="proprietary")

#     try:
#         return Catalog.model_validate({
#             "type": "Catalog",
#             "id": id,
#             "description": description,
#             "stac_version": "1.0.0",
#             "links": [],
#             "title": title,
#             "license": license,
#         }, context="/dev/null")
#     except ValidationError as error:
#         print_error(f"Cannot create catalog", error=error)
#         print(f"\n{style_indent(str(error))}")
#         raise typer.Exit(1)


def load_config(debug: bool = False) -> RepositoriesConfig:
    try:
        return RepositoriesConfig.load()
    except Exception as error:
        print_error(error, no_traceback=not debug)
        raise typer.Exit(1)


def get_backend(
    backend_id: str,
    debug: bool = False
) -> Backend:
    if backend_id not in discovered_backends:
        print_error(f"Backend {backend_id} not found.")
        raise typer.Exit(1)

    return discovered_backends[backend_id]


def load_repository(
    config: RepositoriesConfig,
    debug: bool = False
) -> BaseStacRepository:

    if config.active is None:
        if not config.keys():
            print_error(f"No repository found in .stac-repository.toml")
        else:
            print_error(f"No repository activated. See stac-repository activate --help.")

        raise typer.Exit(1)

    backend = config.active.get_backend()

    try:
        return backend.StacRepository(
            config.active.get_backend_config()
        )
    except RepositoryNotFoundError as error:
        print_error(f"Repository not found.", error=error, no_traceback=not debug)
        raise typer.Exit(1)


app = typer.Typer(no_args_is_help=True)


@app.callback()
def callback():
    """🌍🛰️\tSTAC Repository

    The interface to manage STAC catalogs.
    """


@app.command()
def version():
    """Shows stac-repository version number.
    """
    print(__version__)


@app.command()
def show_repositories(
    debug: bool = False
):
    """Shows configured repositories.
    """

    config = load_config(debug=debug)

    print_list([
        f"{style_bold(repository_id)} (active)" if repository_config.active else repository_id
        for (repository_id, repository_config) in config.items()
    ])


@app.command()
def activate(
    repository: Annotated[
        Optional[str],
        typer.Argument(
            help="The repository to switch to."
        )
    ] = None,
    debug: bool = False
):
    """Switch the activated repository from those listed in .stac-repository.toml
    """

    config = load_config(debug=debug)

    try:
        config.active = repository
        config.save()
    except Exception as error:
        print_error(error, no_traceback=not debug)
        raise typer.Exit(1)


@app.command()
def show_backends(debug: bool = False):
    """Shows installed stac-repository backends.
    """

    print_list([
        f"{backend} version={discovered_backends[backend].__version__}"
        for backend in discovered_backends.keys()
    ])


@app.command()
def show_processors(debug: bool = False):
    """Shows installed stac-repository processors.
    """

    print_list([
        f"{processor} version={discovered_processors[processor].__version__}"
        for processor in discovered_processors.keys()
    ])


@app.command()
def ingest(
    sources: Annotated[
        List[str],
        typer.Argument(help="Sources to ingest.")
    ],
    parent: Annotated[
        Optional[str],
        typer.Option(
            help=(
        "Id of the catalog or collection under which to ingest the products."
        " Defaults to the root catalog if unspecified."
                )
        )
    ] = None,
    processor: Annotated[
        str,
        typer.Option(
            help="Processor (if any) to use to discover and ingest products"
        )
    ] = "stac",
    ingest_assets: Annotated[
        bool,
        typer.Option(
            help="Whever to ingest assets or not."
        )
    ] = True,
    ingest_assets_out_of_scope: Annotated[
        bool,
        typer.Option(
            help="[Danger] Whever to ingest assets out of scope or not. If true then `--ingest-assets` becomes true too."
        )
    ] = False,
    ingest_out_of_scope: Annotated[
        bool,
        typer.Option(
            help="[Danger] Whever to ingest child products out of scope or not."
        )
    ] = False,
    debug: bool = False
):
    """Ingests some products from various sources (eventually using an installed processor).

    If a --processor is specified it will be used to discover and process the products.
    If left unspecified sources must be paths to stac objects (catalog, collection or item).
    """
    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    if parent is None:
        if not prompt.Confirm.ask("No --parent specified, overwrite catalog root ?", default=False):
            return

    try:
        print_reports(
            stac_repository.ingest(
                *sources,
                processor_id=processor,
                parent_id=parent,
                ingest_assets=ingest_assets,
                ingest_assets_out_of_scope=ingest_assets_out_of_scope,
                ingest_out_of_scope=ingest_out_of_scope
            ),
            operation_name="Ingestion [{0}] {1}".format(
                processor,
                sources
            )
        )
    except (
        ProcessorNotFoundError,
        ProcessingError,
        StacObjectError,
        FileNotFoundError,
        HrefError,
        UncatalogError,
        CatalogError,
    ) as error:
        print_error(error, error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except ErrorGroup as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            ProcessorNotFoundError,
            ProcessingError,
            StacObjectError,
            FileNotFoundError,
            HrefError,
            UncatalogError,
            CatalogError,
        ) if not debug else False)
        raise typer.Exit(1)


@app.command()
def prune(
    product_ids: List[str],
    extract: Annotated[
        Optional[str],
        typer.Option(
            help="If specified, must be the path to a directory in which pruned products will be extracted."
        )
    ] = None,
    debug: bool = False
):
    """Removes some products from the catalog.
    """
    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    try:
        print_reports(
            stac_repository.prune(*product_ids, extract=extract),
            operation_name="Deletion"
        )
    except (
        UncatalogError,
    ) as error:
        print_error(error, error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except ErrorGroup as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            UncatalogError,
        ) if not debug else False)
        raise typer.Exit(1)


@app.command()
def history(
    verbose: bool = False,
    debug: bool = False,
):
    """Logs the catalog history.
    """
    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    for commit in stac_repository.commits:
        print(style_list_item(style_commit(commit, include_message=verbose)))


@app.command()
def rollback(
    ref: Annotated[
        str,
        typer.Argument(
            help=(
                "Commit ref."
                # "Either the commit id, "
                # "a datetime (which will rollback to the first commit **before** this date), "
                # "or an integer (0 being the current head, 1 the previous commit, 2 the second previous commit, etc)."
            )
        )
    ],
    debug: bool = False
):
    """Rollbacks the catalog to a previous commit. Support depends on the chosen backend.
    """

    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    try:
        commit = stac_repository.get_commit(ref)
    except CommitNotFoundError as error:
        print_error(f"No commit found matching {ref}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except RefTypeError as error:
        print_error(f"Bad --ref option : {str(error)}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)

    if commit.rollback() == NotImplemented:
        print_error(f"Backend does not support rollbacks.")
        raise typer.Exit(1)


@app.command()
def export(
    dir: Annotated[
        str,
        typer.Argument(help="Export directory.")
    ],
    ref: Annotated[
        Optional[str],
        typer.Option(help="Commit ref.")
    ] = None,
    debug: bool = False
):
    """Exports the catalog. If a commit ref is specified, exports the catalog as it was at that point in time.
    """

    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    if ref is not None:
        try:
            commit = stac_repository.get_commit(ref)
        except CommitNotFoundError as error:
            print_error(f"No commit found matching {ref}.", error=error, no_traceback=not debug)
            raise typer.Exit(1)
        except RefTypeError as error:
            print_error(f"Bad --ref option : {str(error)}.", error=error, no_traceback=not debug)
            raise typer.Exit(1)
    else:
        commit = next(stac_repository.commits)

    try:
        commit.export(dir)
    except FileExistsError as error:
        print_error(f"Export directory is not empty.", error=error, no_traceback=not debug)
        raise typer.Exit(1)


@app.command()
def backup(
    backup: Annotated[
        str,
        typer.Argument(help="Backup URI. Interpreted by the chosen backend.")
    ],
    ref: Annotated[
        Optional[str],
        typer.Option(help="Commit ref.")
    ] = None,
    debug: bool = False
):
    """Backups the repository. If a commit ref is specified, backups the repository only up to this point in time.

    Support depends on the chosen backend.
    """

    config = load_config(debug=debug)
    stac_repository = load_repository(config=config, debug=debug)

    if ref is not None:
        try:
            commit = stac_repository.get_commit(ref)
        except CommitNotFoundError as error:
            print_error(f"No commit found matching {ref}.", error=error, no_traceback=not debug)
            raise typer.Exit(1)
        except RefTypeError as error:
            print_error(f"Bad --ref option : {str(error)}.", error=error, no_traceback=not debug)
            raise typer.Exit(1)
    else:
        commit = next(stac_repository.commits)

    try:
        if commit.backup(backup) == NotImplemented:
            print_error(f"Backend does not support backups.")
            raise typer.Exit(1)
    except BackupValueError as error:
        print_error(f"Bad --backup option : {str(error)}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)
