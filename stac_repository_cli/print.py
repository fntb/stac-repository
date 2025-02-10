from typing import Any, Iterator

import pystac
import pystac.layout
import rich.console
import rich.status
import rich
from rich import print

from stac_repository import JobState
from stac_repository import JobReport
from stac_repository.managed import StacRepositoryExtension
from stac_repository.managed import BaseStacCommitManaged


def indent(s: str) -> str:
    return "\t" + "\n\t".join(s.splitlines())


def list_item(s: str) -> str:
    return " • " + "\n   ".join(s.splitlines())


def job_context_to_rich_str(job_context: Any) -> str:
    return f"[bold]{str(job_context)}[/bold]"


def job_error_to_rich_str(job_error: Any) -> str:
    return f"[bold red]{str(job_error) if isinstance(job_error, Exception) else "Error"}[/bold red]"


def job_result_to_rich_str(job_result: Any):
    return f"[bold green]{job_result if isinstance(job_result, str) else "Done"}[/bold green]"


def job_report_to_rich_str(job_report: JobReport):
    if job_report.state == JobState.INPROGRESS:
        return "🛰️\t{0} : {1}".format(
            job_context_to_rich_str(job_report.context),
            job_report.details
        )
    elif job_report.state == JobState.SUCCESS:
        return list_item(
            "{0} : {1}".format(
                job_context_to_rich_str(job_report.context),
                job_result_to_rich_str(job_report.details)
            )
        )
    else:
        return list_item(
            "{0} : {1}".format(
                job_context_to_rich_str(job_report.context),
                job_error_to_rich_str(job_report.details)
            )
        )


def print_jobs(operation: Iterator[JobReport], *, operation_name=".", console: rich.console.Console | None = None):

    console = console or rich.get_console()
    status = console.status(operation_name, spinner="earth")

    for job_report in operation:
        if job_report.state == JobState.INPROGRESS:
            status.start()
            status.update(job_report_to_rich_str(job_report))
        else:
            status.stop()
            console.print(job_report_to_rich_str(job_report))


def product_to_rich_str(product: pystac.STACObject) -> str:
    return "{id} (version={version}, processor={processor}:{processor_version})".format(
        id=product.id,
        version=StacRepositoryExtension.get_product_version(
            product),
        processor=StacRepositoryExtension.get_processor(
            product),
        processor_version=StacRepositoryExtension.get_processor_version(
            product)
    )


def product_mutation_to_rich_str(product: pystac.STACObject, reprocessed_product: pystac.STACObject):
    return "{id} (version={version}, processor={processor}:{old_processor_version}->{new_processor_version})".format(
        id=product.id,
        version=StacRepositoryExtension.get_product_version(
            product),
        processor=StacRepositoryExtension.get_processor(
            product),
        old_processor_version=StacRepositoryExtension.get_processor_version(
            product),
        new_processor_version=StacRepositoryExtension.get_processor_version(
            reprocessed_product)
    )


def product_mutations_to_rich_str(*product_mutations: tuple[pystac.STACObject | None, pystac.STACObject | None]):
    added_products = [
        product_to_rich_str(b)
        for (a, b)
        in product_mutations
        if a is None and b is not None
    ]
    modified_products = [
        product_mutation_to_rich_str(a, b)
        for (a, b)
        in product_mutations
        if a is not None and b is not None
    ]
    removed_products = [
        product_to_rich_str(a)
        for (a, b)
        in product_mutations
        if a is not None and b is None
    ]

    added_products_str = "[bold green]+[/bold green] {0}".format(
        " ".join(added_products)) if added_products else ""
    modified_products_str = "[bold red]-[/bold red][bold green]+[/bold green] {0}".format(
        " ".join(modified_products)) if modified_products else ""
    removed_products_str = "[bold red]-[/bold red] {0}".format(
        " ".join(removed_products)) if removed_products else ""

    return "\n".join([
        products_str
        for products_str
        in [added_products_str, modified_products_str, removed_products_str]
    ])


def commit_to_rich_str(commit: BaseStacCommitManaged, include_message: bool = False):

    return "[bold]{id}[/bold] on {datetime}\n{message}\n{products}".format(
        id=commit.id,
        datetime=str(commit.datetime),
        message=indent(commit.message) if (
            include_message and commit.message is not None) else "",
        products=indent(
            product_mutations_to_rich_str(
                *(
                    [
                        (None, product)
                        for product
                        in commit.ingested_products
                    ] +
                    list(commit.reprocessed_products) +
                    [
                        (product, None)
                        for product
                        in commit.pruned_products
                    ]
                )
            )
        )
    )
