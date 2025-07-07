from __future__ import annotations

from typing import (
    Iterator,
    Optional,
    TypeVar,
    Type,
    List,
    Dict
)
from abc import (
    abstractmethod,
    ABCMeta
)

import datetime


from .__about__ import __name_public__, __version__

from .stac import (
    Catalog
)

from .base_stac_commit import BaseStacCommit

from .processor import Processor
from .processors import (
    discovered_processors,
)
from .job_report import (
    JobReport,
    JobReportBuilder
)

from .base_stac_transaction import (
    BaseStacTransaction,
    StacObjectError,
    ParentCatalogError,
    RootUncatalogError,
    ParentNotFoundError,
    ObjectNotFoundError
)

from .stac import (
    get_version as _get_version,
    VersionNotFoundError as _VersionNotFoundError
)


class RepositoryAlreadyInitializedError(FileExistsError):
    pass


class RepositoryNotFoundError(FileNotFoundError):
    pass


class CommitNotFoundError(ValueError):
    pass


class RefTypeError(TypeError):
    pass


class ProcessorNotFoundError(ValueError):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_note(
            "Processors : " + (", ".join(discovered_processors.keys()) or "-")
        )


class ProcessingError(Exception):
    pass


class ProcessingErrors(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __len__(self):
        return len(self.__dict__)

    def items(self):
        return self.__dict__.items()


class SkipIteration(Exception):
    pass


_Self = TypeVar("_Self", bound="BaseStacRepository")


class BaseStacRepository(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def init(
        cls: Type[_Self],
        repository: str,
        root_catalog: Catalog,
    ) -> _Self:
        """Create a new repository.

        Raises:
            RepositoryAlreadyInitializedError: If the repository already exists.
        """
        raise NotImplementedError

    def __init__(
        self,
        repository: str,
    ):
        """Open an existing repository.

        Raises:
            RepositoryNotFoundError: If the repository does not exist.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def commits(self) -> Iterator[BaseStacCommit]:
        """Iterates over the commit history, from most to least recent.
        """
        raise NotImplementedError

    def get_commit(self, ref: str | datetime.datetime | int) -> BaseStacCommit:
        """Get a commit matching some ref. Either the commit id, its index from the most recent commit, or the most recent commit before some date.

        Raises:
            CommitNotFoundError: If no, or multiple, commits matching the ref are found.
            RefTypeError: Invalid ref type
        """
        if isinstance(ref, str):
            candidates: List[BaseStacCommit] = []

            for commit in self.commits:
                if commit.id.startswith(ref):
                    candidates.append(commit)

            if not candidates:
                raise CommitNotFoundError
            elif len(candidates) > 1:
                raise CommitNotFoundError(f"Multiple commits found matching ref {ref}")
            else:
                return candidates.pop()
        elif isinstance(ref, int):
            for (i, commit) in enumerate(self.commits):
                if -i == ref:
                    return commit

            raise CommitNotFoundError
        elif isinstance(ref, datetime.datetime):
            for commit in self.commits:
                if commit.datetime <= ref:
                    return commit

            raise CommitNotFoundError
        else:
            raise RefTypeError("Bad ref")

    def get_history(self, product_id: str | None = None) -> Iterator[BaseStacCommit]:
        """Iterates over the commit history, yielding only those where some product can be found in the catalog.
        """
        raise NotImplementedError

    def _ingest_product(
        self,
        processor: Processor,
        product_source: str,
        parent_id: Optional[str] = None,
        *,
        transaction: BaseStacTransaction,
    ) -> Iterator[JobReport]:
        """Ingests a product.

        Raises:
            ProcessingError: The processor raised an error
            StacObjectError: The processed product is not a valid Item or Collection, or cannot compute the parent new extent
            ParentNotFoundError
            ParentCatalogError: The parent is an Item
        """
        reporter = JobReportBuilder(product_source)

        yield reporter.progress("Identifying & versionning")

        try:
            try:
                product_id = processor.id(product_source)
                product_version = processor.version(product_source)
            except Exception as error:
                raise ProcessingError from error

            cataloged_stac_object = transaction.search(product_id)

            if cataloged_stac_object is not None:
                try:
                    if product_version == _get_version(cataloged_stac_object):
                        raise SkipIteration
                except _VersionNotFoundError as error:
                    yield reporter.progress("Product found but unversionned, reprocessing")
                else:
                    yield reporter.progress("Previous version of the product found, reprocessing")
            else:
                yield reporter.progress("Product not found, processing")

            try:
                processed_stac_object_file = processor.process(product_source)
            except Exception as error:
                raise ProcessingError from error

            yield reporter.progress("Cataloging")

            transaction.catalog(
                processed_stac_object_file,
                parent_id=parent_id
            )

            yield reporter.complete("Cataloged")
        except SkipIteration:
            yield reporter.complete("Product is already cataloged with matching version, skipping")
        except Exception as error:
            yield reporter.fail(error)
            raise error

    def _ingest(
        self,
        *sources: str,
        processor_id: str = "none",
        parent_id: Optional[str] = None,
        transaction_cls: type[BaseStacTransaction]
    ) -> Iterator[JobReport]:
        """Discover and ingest products from some source(s).

        Raises:
            ProcessorNotFoundError
            ProcessingError: The processor raised an error
            StacObjectError: The processed product is not a valid Item or Collection, or cannot compute the parent new extent
            ParentNotFoundError
            ParentCatalogError: The parent is an Item
            Dict[str, Exception]: Map of source/product_source to Exceptions (any of the above)
        """
        processor: Processor = discovered_processors.get(processor_id)

        if processor is None:
            raise ProcessorNotFoundError(processor_id)

        product_sources = []

        errors = ProcessingErrors()

        for source in sources:
            reporter = JobReportBuilder(source)
            yield reporter.progress(f"Discovering products from {source}")

            try:
                discovered_product_sources = list(processor.discover(source))
                product_sources.extend(discovered_product_sources)
            except Exception as error:
                try:
                    raise ProcessingError from error
                except ProcessingError as error:
                    yield reporter.fail(error)
                    errors[f"source={source}"] = error
            else:
                if discovered_product_sources:
                    yield reporter.complete(f"Discovered products {' '.join(discovered_product_sources)}")
                else:
                    yield reporter.complete(f"No products discovered")

        with transaction_cls(self).context(
            message=f"{processor_id} ingestion : \n\n - " + "\n - ".join(product_sources)
        ) as transaction:
            for product_source in product_sources:
                try:
                    yield from self._ingest_product(
                        processor=processor,
                        product_source=product_source,
                        parent_id=parent_id,
                        transaction=transaction,
                    )
                except Exception as error:
                    errors[f"product={product_source}"] = error

        if errors:
            raise errors

    @abstractmethod
    def ingest(
        self,
        *sources: str,
        processor_id: str = "none",
        parent_id: Optional[str] = None,
    ) -> Iterator[JobReport]:
        """Discover and ingest products from some source(s) into the catalog.

        Raises:
            ProcessorNotFoundError
            ProcessingError: The processor raised an error
            StacObjectError: The processed product is not a valid Item or Collection, or cannot compute the parent new extent
            ParentNotFoundError
            ParentCatalogError: The parent is an Item
            Dict[str, Exception]: Map of source/product_source to Exceptions (any of the above)
        """

        # This method is just a wrapper for concrete implementations to call _ingest() with the proper StacTransaction type
        raise NotImplementedError

    def _prune(
        self,
        *product_ids: str,
        transaction_cls: type[BaseStacTransaction]
    ) -> Iterator[JobReport]:
        """Removes some product(s) from the catalog.

        Raises:
            RootUncatalogError: The product is the catalog root
            StacObjectError: Cannot compute the parent new extent
            Dict[str, Exception]: Map of ids to Exceptions (any of the above)
        """
        errors = ProcessingErrors()

        transaction_message = "prune : \n\n - " + "\n - ".join(product_ids)
        with transaction_cls(self).context(message=transaction_message) as transaction:
            for product_id in product_ids:
                reporter = JobReportBuilder(product_id)

                yield reporter.progress("Pruning")

                try:
                    yield reporter.progress("Uncataloging")

                    try:
                        transaction.uncatalog(product_id)
                    except ObjectNotFoundError:
                        yield reporter.complete("Not found in catalog")
                    else:
                        yield reporter.complete("Uncataloged")
                except Exception as error:
                    yield reporter.fail(error)

                    errors[product_id] = error

        if errors:
            raise errors

    @abstractmethod
    def prune(
        self,
        *product_ids: str
    ) -> Iterator[JobReport]:
        """Removes some product(s) from the catalog.

        Raises:
            RootUncatalogError: The product is the catalog root
            StacObjectError: Cannot compute the parent new extent
            Dict[str, Exception]: Map of ids to Exceptions (any of the above)
        """
        # This method is just a wrapper for concrete implementations to call _prune() with the proper StacTransaction type
        raise NotImplementedError

    def export(
        self,
        cls: Optional[BaseStacRepository] = None
    ):
        """Exports the catalog to another backend or the filesystem."""
        raise NotImplementedError
