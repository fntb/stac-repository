from typing import Iterator, Any

import pystac


from .processor import Processor
from .processors import discovered_processors
from ..base_stac_repository import BaseStacRepository
from ..lib.job_report import JobReport
from ..lib.job_report import JobReporter
from ..base_stac_transaction import BaseStacTransaction
from .stac_extension import StacRepositoryExtension
from .stac_commit_managed_mixin import StacCommitManagedMixin


class ProcessorNotFoundError(ValueError):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_note(
            "Processors : " + (", ".join(discovered_processors.keys()) or "-")
        )


class ProcessingError(Exception):

    _product: str
    _processor: str | None

    def __init__(self, cause: Exception, product: str, *, processor: str | None = None):
        super().__init__()
        self.__cause__ = cause
        self.with_traceback(cause.__traceback__)

        self._product = product
        self._processor = processor

    @property
    def product(self) -> str:
        return self._product

    @property
    def processor(self) -> str:
        return self._processor


class ProcessingErrorGroup(ExceptionGroup):
    def __new__(cls, message, excs=None):
        if not isinstance(message, str):
            excs = message
            message = "Some product(s) couldn't be processed"

        obj = super().__new__(cls, message, excs)
        return obj


class SkipIteration(StopIteration):
    pass


class StacRepositoryManagedMixin(BaseStacRepository):

    def discover(
        self,
        processor_id: str,
        source: str
    ) -> Iterator[str]:
        processor: Processor = discovered_processors.get(processor_id)

        if processor is None:
            raise ProcessorNotFoundError(processor_id)

        yield from processor.discover(source)

    def ingest_products(
        self,
        processor_id: str,
        *product_sources: list[str],
        transaction_type: type[BaseStacTransaction]
    ) -> Iterator[JobReport]:
        processor: Processor = discovered_processors.get(processor_id)

        if processor is None:
            raise ProcessorNotFoundError(processor_id)

        errors = list()
        transaction_message = processor_id + \
            " ingestion : \n\n - " + "\n - ".join(product_sources)
        with transaction_type(self).context(message=transaction_message) as transaction:
            for product_source in product_sources:
                reporter = JobReporter(product_source)

                yield reporter.progress("Identifying & versionning")

                try:
                    product_id = processor.id(product_source)
                    product_version = processor.version(product_source)

                    cataloged_stac_object = transaction.catalog.get_child(
                        product_id,
                        recursive=True
                    )

                    if cataloged_stac_object is not None and product_version == StacRepositoryExtension.get_product_version(cataloged_stac_object):
                        raise SkipIteration

                    yield reporter.progress("Processing")

                    processed_stac_object_file = processor.process(
                        product_source)

                    stac_object = pystac.read_file(processed_stac_object_file)
                    StacRepositoryExtension.implement(
                        stac_object,
                        processor_id=processor_id,
                        processor_version=processor.__version__,
                        product_version=product_version
                    )
                    stac_object.save_object()

                    yield reporter.progress("Cataloging")

                    processor.catalog(
                        processed_stac_object_file,
                        catalog_file=transaction.catalog.get_self_href()
                    )

                    yield reporter.progress("Staging")

                    transaction.stage(product_id)

                    yield reporter.complete("Ingested")
                except SkipIteration:
                    yield reporter.complete(
                        "Product is already cataloged with matching version, skipping")
                except Exception as error:
                    error = ProcessingError(
                        error,
                        product_source,
                        processor=processor_id
                    )

                    yield reporter.fail(error)

                    errors.append(
                        ProcessingError(
                            error,
                            product_source,
                            processor=processor_id
                        )
                    )

        if errors:
            raise ProcessingErrorGroup(errors)

    def ingest(
        self,
        processor_id: str,
        source: Any,
        *,
        transaction_type: type[BaseStacTransaction]
    ) -> Iterator[JobReport]:
        return self.ingest_products(
            processor_id,
            *self.discover(
                processor_id,
                source
            ),
            transaction_type=transaction_type
        )

    def prune(
        self,
        *product_ids: str,
        transaction_type: type[BaseStacTransaction]
    ) -> Iterator[JobReport]:
        errors = list()

        transaction_message = "prune : \n\n - " + "\n - ".join(product_ids)
        with transaction_type(self).context(message=transaction_message) as transaction:
            for product_id in product_ids:
                reporter = JobReporter(product_id)

                yield reporter.progress("Pruning")

                try:
                    stac_object = transaction.catalog.get_child(
                        product_id,
                        recursive=True
                    )

                    if stac_object is None:
                        raise SkipIteration

                    processor_id = StacRepositoryExtension.get_processor(
                        stac_object)

                    if processor_id is None:
                        raise ProcessorNotFoundError(
                            f"""Product doesn't seem to have a processor field"""
                        )

                    processor = discovered_processors.get(processor_id)
                    if processor is None:
                        raise ProcessorNotFoundError(
                            f"""Product processor {processor_id} not loaded"""
                        )
                    else:
                        yield reporter.progress(
                            f"Identified processor {processor_id=}")

                    try:
                        yield reporter.progress("Uncataloging")

                        processor.uncatalog(
                            product_id,
                            catalog_file=transaction.catalog.get_self_href()
                        )

                        yield reporter.progress("Staging")

                        transaction.stage(product_id)

                        yield reporter.complete("Pruned")
                    except Exception as error:
                        transaction.abort_unstaged()
                        raise error
                except SkipIteration:
                    yield reporter.complete("Not found in catalog")
                except Exception as error:
                    error = ProcessingError(
                        error,
                        product_id
                    )

                    yield reporter.fail(error)

                    errors.append(
                        ProcessingError(
                            error,
                            product_id
                        )
                    )

        if errors:
            raise ProcessingErrorGroup(errors)

    def history(self, product_id: str | None = None) -> Iterator[StacCommitManagedMixin]:
        """Inspect the database history"""
        raise NotImplementedError
