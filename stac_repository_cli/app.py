from typing import (
    Annotated,
    Optional,
    List,
)

import pystac

import typer

import rich
import rich.console
import rich.status
from rich import print
from rich import prompt

from stac_repository import (
    __version__,
    discovered_processors,
    BaseStacRepository,
    RepositoryAlreadyInitializedError,
    RepositoryNotFoundError,
    CommitNotFoundError,
    Backend,
    BackupValueError,
    RefTypeError,
    ProcessorNotFoundError,
    ProcessingError,
    ProcessingErrors,
    StacObjectError,
    ParentNotFoundError,
    ParentCatalogError,
    RootUncatalogError,
)

from stac_repository_cli.backends import discovered_backends
from .print import print_reports, print_error, print_list
# from .print import commit_to_rich_str
# from .print import list_item


class BackendNotFoundError(ValueError):
    ...


def get_backend(backend_id: str) -> Backend:
    if backend_id not in discovered_backends:
        print_error(f"Backend {backend_id} not found.")
        raise typer.Exit(1)

    return discovered_backends[backend_id]


def init_repository(
    backend_id: str,
    repository: str,
    root_catalog: str | pystac.Catalog
) -> BaseStacRepository:

    backend = get_backend(backend_id)

    try:
        return backend.StacRepository.init(
            repository,
            root_catalog
        )
    except RepositoryAlreadyInitializedError as error:
        print_error(f"Repository {repository} already initialized.", error=error)
        raise typer.Exit(1)


def load_repository(
    backend_id: str,
    repository: str
) -> BaseStacRepository:

    backend = get_backend(backend_id)

    try:
        return backend.StacRepository(
            repository
        )
    except RepositoryNotFoundError as error:
        print_error(f"Repository {repository} not found.", error=error)
        raise typer.Exit(1)


app = typer.Typer(pretty_exceptions_enable=False)


@app.callback()
def callback():
    """🌍🛰️\tSTAC Repository

    The interface to manage STAC Repositories.
    """


@app.command()
def version():
    """Show stac-repository version number.
    """
    print(__version__)


@app.command()
def show_backends():
    """Show installed stac-repository backends.
    """

    print_list([
        f"{backend} version={discovered_backends[backend].__version__}"
        for backend in discovered_backends.keys()
    ])


@app.command()
def show_processors():
    """Show installed stac-repository processors.
    """

    print_list([
        f"{processor} version={discovered_processors[processor].__version__}"
        for processor in discovered_processors.keys()
    ])


@app.command()
def init(
    repository: Annotated[
        str,
        typer.Argument(help="Repository URI. Interpreted by the chosen backend.")
    ],
    backend: str = "file",
    root_catalog: Annotated[
        Optional[str],
        typer.Option(
            help="Existing root catalog to initialize the repository from. Leave out to use the interactive initializer.")
    ] = None
):
    """Initialize the repository.
    """

    root_catalog_instance: pystac.Catalog

    if not root_catalog:
        root_catalog = prompt.Prompt.ask(
            "Initialize from an existing root catalog file ?",
            default="Leave blank to use the interactive initializer"
        )

        if root_catalog == "Leave blank to use the interactive initializer":
            root_catalog = None

            id = prompt.Prompt.ask("id", default="root")
            title = prompt.Prompt.ask("title")
            description = prompt.Prompt.ask("description")
            license = prompt.Prompt.ask("license", default="proprietary")

            root_catalog_instance = pystac.Catalog(
                id,
                description,
                title,
                extra_fields={
                    "license": license
                }
            )

            print(root_catalog_instance.to_dict(include_self_link=False))

    if root_catalog:
        root_catalog_instance = pystac.Catalog.from_file(root_catalog)
        print(root_catalog_instance.to_dict(include_self_link=False))

    if not prompt.Confirm.ask("Use as root catalog ?", default=False):
        return

    init_repository(
        backend_id=backend,
        repository=repository,
        root_catalog=root_catalog_instance
    )


@app.command()
def ingest(
    repository: Annotated[
        str,
        typer.Argument(help="Repository URI. Interpreted by the chosen backend.")
    ],
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
    backend: Optional[str] = "file",
    processor: Annotated[
        Optional[str],
        typer.Option(
            help="Processor (if any) to use to discover and ingest products"
        )
    ] = "none",
):
    """Ingest products from various sources (eventually using an installed processor).

    If a --processor is specified it will be used to discover and process the products.
    If left unspecified sources must be paths to stac objects (catalog, collection or item).
    """
    stac_repository = load_repository(backend, repository)

    try:
        print_reports(
            stac_repository.ingest(
                *sources,
                processor_id=processor,
                parent_id=parent,
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
        ParentNotFoundError,
        ParentCatalogError
    ) as error:
        print_error(error, error=error)
        raise typer.Exit(1)
    except ProcessingErrors as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            ProcessorNotFoundError,
            ProcessingError,
            StacObjectError,
            ParentNotFoundError,
            ParentCatalogError
        ))
        raise typer.Exit(1)


@app.command()
def prune(
    repository: Annotated[
        str,
        typer.Argument(help="Repository URI. Interpreted by the chosen backend.")
    ],
    product_ids: list[str],
    backend: Optional[str] = "file",
):
    """Remove products from the catalog.
    """
    stac_repository = load_repository(backend, repository)

    try:
        print_reports(
            stac_repository.prune(*product_ids),
            operation_name="Deletion"
        )
    except (
        RootUncatalogError,
        StacObjectError
    ) as error:
        print_error(error, error=error)
        raise typer.Exit(1)
    except ProcessingErrors as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            RootUncatalogError,
            StacObjectError,
        ))
        raise typer.Exit(1)


# @app.command()
# def history(
#     product_id: Annotated[Optional[str], typer.Argument()] = None,
#     verbose: bool = False,
#     config: str = "stac_repository.toml",
#     git: bool = True,
# ):
#     """Display the catalog history.

#     If --product_id is specified, then filter the history for commits where the product existed in the catalog
#     """
#     stac_repository = load_repository(config, mock=not git)

#     console = rich.get_console()

#     if product_id:
#         console.print(f"[bold]Product History {product_id}[/bold]")
#     else:
#         console.print(f"[bold]History[/bold]")

#     for commit in stac_repository.history(product_id):
#         console.print(list_item(commit_to_rich_str(
#             commit,
#             include_message=verbose
#         )))


@app.command()
def rollback(
    repository: Annotated[
        str,
        typer.Argument(help="Repository URI. Interpreted by the chosen backend.")
    ],
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
    backend: str = "file",
):
    """Rollback the catalog to a previous commit. Support depends on the chosen backend.
    """

    stac_repository = load_repository(backend, repository)
    try:
        commit = stac_repository.get_commit(ref)
    except CommitNotFoundError as error:
        print_error(f"No commit found matching {ref}.", error=error)
        raise typer.Exit(1)
    except RefTypeError as error:
        print_error(f"Bad --ref option : {str(error)}.", error=error)
        raise typer.Exit(1)

    if commit.rollback() == NotImplemented:
        print_error(f"Backend {backend} does not support rollbacks.")
        raise typer.Exit(1)


@app.command()
def backup(
    repository: Annotated[
        str,
        typer.Argument(help="Repository URI. Interpreted by the chosen backend.")
    ],
    backup: Annotated[
        str,
        typer.Argument(help="Backup URI. Interpreted by the chosen backend.")
    ],
    ref: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Commit ref."
                # "Either the commit id, "
                # "a datetime (which will rollback to the first commit **before** this date), "
                # "or an integer (0 being the current head, 1 the previous commit, 2 the second previous commit, etc)."
            )
        )
    ] = None,
    backend: str = "file",
):
    """Clone (or pull) the repository **to** a backup location. Support depends on the chosen backend.
    """

    stac_repository = load_repository(backend, repository)

    if ref is not None:
        try:
            commit = stac_repository.get_commit(ref)
        except CommitNotFoundError as error:
            print_error(f"No commit found matching {ref}.", error=error)
            raise typer.Exit(1)
        except RefTypeError as error:
            print_error(f"Bad --ref option : {str(error)}.", error=error)
            raise typer.Exit(1)
    else:
        commit = next(stac_repository.commits)

    try:
        if commit.backup(backup) == NotImplemented:
            print_error(f"Backend {backend} does not support backups.")
            raise typer.Exit(1)
    except BackupValueError as error:
        print_error(f"Bad --backup option : {str(error)}.", error=error)
        raise typer.Exit(1)
