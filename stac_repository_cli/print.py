from typing import (
    Iterator,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

import traceback

import rich.console
import rich.status
import rich
from rich import print

from stac_repository import JobState
from stac_repository import JobReport
from stac_repository import (
    Catalog,
    Item,
    Collection,
    BaseStacCommit
)
from stac_repository.stac import get_version as _get_version


def style_indent(s: str | List[str]) -> str:
    return "   " + "\n   ".join(s.splitlines() if isinstance(s, str) else s)


def style_bold(s: str) -> str:
    return f"[bold]{s}[/bold]"


def style_red(s: str) -> str:
    return f"[red]{s}[/red]"


def style_green(s: str) -> str:
    return f"[green]{s}[/green]"


def style_list_item(s: str | List[str]) -> str:
    return " • " + style_indent(s).strip()


def style_list(l: List[str | List[str]]) -> str:
    return "\n".join(style_list_item(s) for s in l)


def print_list(l: List[str | List[str]], console: rich.console.Console | None = None):
    console = console or rich.get_console()

    console.print(style_list(l))


def style_error(
    message: BaseException | str,
    *,
    error: Optional[BaseException] = None,
    no_traceback: Optional[bool | Type[BaseException] | Tuple[Type[BaseException]]] = True,
) -> str:
    error_str: str
    traceback_str: str = ""

    if isinstance(message, str):
        if error is not None:
            error_str = f"[{type(error).__name__}] {message}"

            if no_traceback is not True and (no_traceback is False or not isinstance(error, no_traceback)):
                traceback_str = "\n" + "\n".join(traceback.format_exception(error))
        else:
            error_str = message
    else:
        error = message
        error_str = f"[{type(error).__name__}] {str(error)}"

        if no_traceback is not True and (no_traceback is False or not isinstance(error, no_traceback)):
            traceback_str = "\n" + "\n".join(traceback.format_exception(error))

    return style_red(style_bold(error_str)) + traceback_str


def style_errors(
    errors: Dict[str, BaseException | str],
    no_traceback: Optional[bool | Type[BaseException] | Tuple[Type[BaseException]]] = True,
) -> str:
    return style_list(
        [
            "{0} : {1}".format(
                style_bold(key),
                style_error(error, no_traceback=no_traceback)
            )
            for (key, error) in errors.items()
        ]
    )


def style_report(job_report: JobReport):
    if job_report.state == JobState.INPROGRESS:
        return "🛰️ {0} : {1}".format(
            style_bold(job_report.context),
            job_report.details
        )
    elif job_report.state == JobState.SUCCESS:
        return style_list_item(
            "{0} : {1}".format(
                style_bold(job_report.context),
                style_green(style_bold(job_report.details))
            )
        )
    else:
        return style_list_item(
            "{0} : {1}".format(
                style_bold(job_report.context),
                style_error(job_report.details)
            )
        )


def print_reports(operation: Iterator[JobReport], *, operation_name=".", console: rich.console.Console | None = None):

    console = console or rich.get_console()
    status = console.status(operation_name, spinner="earth")

    for job_report in operation:
        if job_report.state == JobState.INPROGRESS:
            status.start()
            status.update(style_report(job_report))
        else:
            status.stop()
            console.print(style_report(job_report), crop=False, overflow="ignore")


def style_stac(obj: Item | Collection | Catalog) -> str:
    return "{id} v{version}".format(
        id=obj.id,
        version=_get_version(obj)
    )


def print_error(
    message: Exception | str | Dict[str, Exception | str],
    *,
    console: rich.console.Console | None = None,
    error: Optional[BaseException] = None,
    no_traceback: Optional[bool | Type[BaseException] | Tuple[Type[BaseException]]] = True,
):

    err_console = console or rich.console.Console(stderr=True)

    if hasattr(message, "items"):
        err_console.print(style_errors(message, no_traceback=no_traceback))
    else:
        err_console.print(style_error(message, error=error, no_traceback=no_traceback))


# def product_mutation_to_rich_str(product: pystac.STACObject, reprocessed_product: pystac.STACObject):
#     return "{id} (version={version}, processor={processor}:{old_processor_version}->{new_processor_version})".format(
#         id=product.id,
#         version=StacRepositoryExtension.get_product_version(
#             product),
#         processor=StacRepositoryExtension.get_processor(
#             product),
#         old_processor_version=StacRepositoryExtension.get_processor_version(
#             product),
#         new_processor_version=StacRepositoryExtension.get_processor_version(
#             reprocessed_product)
#     )


# def product_mutations_to_rich_str(*product_mutations: tuple[pystac.STACObject | None, pystac.STACObject | None]):
#     added_products = [
#         product_to_rich_str(b)
#         for (a, b)
#         in product_mutations
#         if a is None and b is not None
#     ]
#     modified_products = [
#         product_mutation_to_rich_str(a, b)
#         for (a, b)
#         in product_mutations
#         if a is not None and b is not None
#     ]
#     removed_products = [
#         product_to_rich_str(a)
#         for (a, b)
#         in product_mutations
#         if a is not None and b is None
#     ]

#     added_products_str = "[bold green]+[/bold green] {0}".format(
#         " ".join(added_products)) if added_products else ""
#     modified_products_str = "[bold red]-[/bold red][bold green]+[/bold green] {0}".format(
#         " ".join(modified_products)) if modified_products else ""
#     removed_products_str = "[bold red]-[/bold red] {0}".format(
#         " ".join(removed_products)) if removed_products else ""

#     return "\n".join([
#         products_str
#         for products_str
#         in [added_products_str, modified_products_str, removed_products_str]
#     ])


# def commit_to_rich_str(commit: BaseStacCommitManaged, include_message: bool = False):

#     return "[bold]{id}[/bold] on {datetime}\n{message}\n{products}".format(
#         id=commit.id,
#         datetime=str(commit.datetime),
#         message=indent(commit.message) if (
#             include_message and commit.message is not None) else "",
#         products=indent(
#             product_mutations_to_rich_str(
#                 *(
#                     [
#                         (None, product)
#                         for product
#                         in commit.ingested_products
#                     ] +
#                     list(commit.reprocessed_products) +
#                     [
#                         (product, None)
#                         for product
#                         in commit.pruned_products
#                     ]
#                 )
#             )
#         )
#     )

def style_commit(commit: BaseStacCommit, include_message: bool = False):

    if include_message and commit.message is not NotImplemented:
        return "[bold]{id}[/bold] on {datetime}\n{message}".format(
            id=commit.id,
            datetime=f"{commit.datetime:%c}",
            message=style_indent(commit.message)
        )
    else:
        return "[bold]{id}[/bold] on {datetime}".format(
            id=commit.id,
            datetime=f"{commit.datetime:%c}",
        )
