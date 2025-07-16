from typing import (
    Annotated,
    Optional,
    List,
    Tuple
)

from types import (
    SimpleNamespace
)

import logging

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
    RootCatalogError,
    ObjectNotFoundError,
    Catalog,
    ConfigError
)

from stac_repository_cli.backends import discovered_backends

from .print import (
    print_reports,
    print_error,
    print_list,
    style_indent,
    style_list_item,
    style_commit
)

install(show_locals=True)

logging.basicConfig(
    level="WARNING", format="%(message)s", datefmt="\t", handlers=[RichHandler()]
)


class BackendNotFoundError(ValueError):
    pass


def get_backend(
    backend_id: str,
    debug: bool = False
) -> Backend:
    if backend_id not in discovered_backends:
        print_error(f"Backend {backend_id} not found.")
        raise typer.Exit(1)

    return discovered_backends[backend_id]


def init_repository(
    backend_id: str,
    repository: str | None,
    root_catalog: Catalog,
    debug: bool = False
) -> BaseStacRepository:
    if repository is None:
        print_error(f"Missing --repository option.")
        raise typer.Exit(1)

    backend = get_backend(backend_id, debug=debug)

    try:
        return backend.StacRepository.init(
            repository,
            root_catalog
        )
    except RepositoryAlreadyInitializedError as error:
        print_error(f"Repository {repository} already initialized.", error=error, no_traceback=not debug)
        raise typer.Exit(1)


def load_repository(
    backend_id: str,
    repository: str | None,
    debug: bool = False
) -> BaseStacRepository:
    if repository is None:
        print_error(f"Missing --repository option.")
        raise typer.Exit(1)

    backend = get_backend(backend_id, debug=debug)

    try:
        return backend.StacRepository(
            repository
        )
    except RepositoryNotFoundError as error:
        print_error(f"Repository {repository} not found.", error=error, no_traceback=not debug)
        raise typer.Exit(1)


app = typer.Typer(no_args_is_help=True)


@app.callback()
def callback(
    context: typer.Context,
    repository: Annotated[
        Optional[str],
        typer.Option(help="Repository URI - interpreted by the chosen backend.")
    ] = None,
    backend: Annotated[
        str,
        typer.Option(help="Backend."),
    ] = "file",
):
    """🌍🛰️\tSTAC Repository

    The interface to manage STAC catalogs.
    """

    context.obj = SimpleNamespace(
        repository=repository,
        backend=backend
    )


@app.command()
def version():
    """Shows stac-repository version number.
    """
    print(__version__)


@app.command()
def show_backends():
    """Shows installed stac-repository backends.
    """

    print_list([
        f"{backend} version={discovered_backends[backend].__version__}"
        for backend in discovered_backends.keys()
    ])


@app.command()
def show_processors():
    """Shows installed stac-repository processors.
    """

    print_list([
        f"{processor} version={discovered_processors[processor].__version__}"
        for processor in discovered_processors.keys()
    ])


@app.command()
def init(
    context: typer.Context,
    root_catalog: Annotated[
        Optional[str],
        typer.Option(
            help="Existing root catalog to initialize the repository from. Leave out to use the interactive initializer.")
    ] = None,
    debug: bool = False
):
    """Initializes the repository.
    """
    root_catalog_instance: Catalog

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

            try:
                root_catalog_instance = Catalog.model_validate({
                    "type": "Catalog",
                    "id": id,
                    "description": description,
                    "stac_version": "1.0.0",
                    "links": [],
                    "title": title,
                    "license": license,
                }, context="/dev/null")
            except ValidationError as error:
                print_error(f"Cannot create catalog", error=error)
                print(f"\n{style_indent(str(error))}")
                raise typer.Exit(1)

            print(root_catalog_instance.model_dump())

    if root_catalog:
        try:
            with open(root_catalog, "r") as catalog_stream:
                root_catalog_instance = Catalog.model_validate_json(catalog_stream, context=root_catalog)
        except ValidationError as error:
            print_error(f"Cannot instanciate catalog", error=error)
            print(f"\n{style_indent(str(error))}")
            raise typer.Exit(1)
        except Exception as error:
            print_error(f"Cannot instanciate catalog {root_catalog}", error=error, no_traceback=not debug)
            raise typer.Exit(1)

        print(root_catalog_instance.model_dump())

    if not prompt.Confirm.ask("Use as root catalog ?", default=False):
        return

    init_repository(
        backend_id=context.obj.backend,
        repository=context.obj.repository,
        root_catalog=root_catalog_instance,
        debug=debug
    )


@app.command()
def config(
    context: typer.Context,
    set: Annotated[
        Optional[Tuple[str, str]],
        typer.Option(
            help="Configuration option.")
    ] = None,
    debug: bool = False
):
    """Get or set the repository configuration options - interpreted by the chosen backend."""
    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)
    try:
        if set is None:
            print(stac_repository.get_config().model_dump())
        else:
            stac_repository.set_config(set[0], set[1])
    except ConfigError as error:
        print_error(error, error=error, no_traceback=not debug)
        raise typer.Exit(1)


@app.command()
def ingest(
    context: typer.Context,
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
        Optional[str],
        typer.Option(
            help="Processor (if any) to use to discover and ingest products"
        )
    ] = "stac",
    debug: bool = False
):
    """Ingests some products from various sources (eventually using an installed processor).

    If a --processor is specified it will be used to discover and process the products.
    If left unspecified sources must be paths to stac objects (catalog, collection or item).
    """
    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)

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
        ParentCatalogError,
        ObjectNotFoundError,
        RootCatalogError
    ) as error:
        print_error(error, error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except ProcessingErrors as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            ProcessorNotFoundError,
            ProcessingError,
            StacObjectError,
            ParentNotFoundError,
            ParentCatalogError,
            ObjectNotFoundError,
            RootCatalogError
        ) if not debug else False)
        raise typer.Exit(1)


@app.command()
def prune(
    context: typer.Context,
    product_ids: list[str],
    debug: bool = False
):
    """Removes some products from the catalog.
    """
    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)

    try:
        print_reports(
            stac_repository.prune(*product_ids),
            operation_name="Deletion"
        )
    except (
        RootUncatalogError,
        ParentNotFoundError
    ) as error:
        print_error(error, error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except ProcessingErrors as errors:
        print(f"\nErrors : \n")
        print_error(errors, no_traceback=(
            RootUncatalogError,
            ParentNotFoundError,
        ) if not debug else False)
        raise typer.Exit(1)


@app.command()
def history(
    context: typer.Context,
    verbose: bool = False,
    debug: bool = False,
):
    """Logs the catalog history.
    """
    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)

    for commit in stac_repository.commits:
        print(style_list_item(style_commit(commit, include_message=verbose)))


@app.command()
def rollback(
    context: typer.Context,
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

    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)
    try:
        commit = stac_repository.get_commit(ref)
    except CommitNotFoundError as error:
        print_error(f"No commit found matching {ref}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)
    except RefTypeError as error:
        print_error(f"Bad --ref option : {str(error)}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)

    if commit.rollback() == NotImplemented:
        print_error(f"Backend {context.obj.backend} does not support rollbacks.")
        raise typer.Exit(1)


@app.command()
def export(
    context: typer.Context,
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

    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)

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
    context: typer.Context,
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

    stac_repository = load_repository(context.obj.backend, context.obj.repository, debug=debug)

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
            print_error(f"Backend {context.obj.backend} does not support backups.")
            raise typer.Exit(1)
    except BackupValueError as error:
        print_error(f"Bad --backup option : {str(error)}.", error=error, no_traceback=not debug)
        raise typer.Exit(1)
