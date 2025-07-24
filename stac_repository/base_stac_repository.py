from __future__ import annotations

from typing import (
    Iterator,
    Optional,
    TypeVar,
    Type,
    List,
    Dict,
    Union,
    Any,
)

import sys
import os
import shutil


from abc import (
    abstractmethod,
    ABCMeta
)

import datetime

from pydantic import (
    BaseModel,
    TypeAdapter,
    ValidationError
)


from .__about__ import __name_public__, __version__

from .stac import (
    Catalog
)

from .base_stac_commit import (
    BaseStacCommit,
    BackupValueError
)

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
    HrefError,
    CatalogError,
    UncatalogError,
    ExtractError
)

from .stac import (
    get_version as _get_version,
    VersionNotFoundError as _VersionNotFoundError
)


class RepositoryNotFoundError(FileNotFoundError):
    pass


class CommitNotFoundError(ValueError):
    pass


class RefTypeError(TypeError):
    pass


class ProcessorNotFoundError(ValueError):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if sys.version_info >= (3, 11):
            self.add_note("Processors : " + (", ".join(discovered_processors.keys()) or "-"))


class ProcessingError(Exception):
    """Wrapper exception type for any type of error raised by a processor."""
    pass


class ErrorGroup(Exception):

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


class BaseStacRepository(metaclass=ABCMeta):

    StacConfig: Type[BaseModel]
    StacTransaction: Type[BaseStacTransaction] = BaseStacTransaction
    StacCommit: Type[BaseStacCommit] = BaseStacCommit

    @abstractmethod
    def __init__(
        self,
        config: BaseModel
    ):
        """Interfaces with the underlying repository.

        Raises:
            RepositoryNotFoundError: If the repository does not exist.
        """
        raise NotImplementedError

    @property
    def commits(self) -> Iterator[BaseStacCommit]:
        """Iterates over the commit history, from most to least recent.
        """
        commit = self.StacCommit(self)
        yield commit
        while (commit := commit.parent) is not None:
            yield commit

    def get_commit(self, ref: Union[str, datetime.datetime, int]) -> BaseStacCommit:
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

    def ingest(
        self,
        *sources: str,
        processor_id: str = "stac",
        parent_id: Optional[str] = None,
        ingest_assets: bool = False,
        ingest_assets_out_of_scope: bool = False,
        ingest_out_of_scope: bool = False,
    ) -> Iterator[JobReport]:
        """Discover and ingest products from some source(s).

        Raises:
            ProcessorNotFoundError:
            ErrorGroup:
            ProcessingError: Processor raised an error
            FileNotFoundError: Product source does not exist
            StacObjectError: Product source is not a valid STAC object
            HrefError:  Product source href (scheme) cannot be processed
            UncatalogError:
                - Old product version couldn't be deleted sucessfully
                - Cannot uncatalog the root
            CatalogError:
        """
        processor: Optional[Processor] = discovered_processors.get(processor_id)

        if processor is None:
            raise ProcessorNotFoundError(processor_id)

        product_sources = []

        errors = ErrorGroup()

        for source in sources:
            reporter = JobReportBuilder(source)
            yield reporter.progress(f"Discovering products from {source}")

            try:
                discovered_product_sources = list(processor.discover(source))
                product_sources.extend(discovered_product_sources)
            except Exception as error:
                try:
                    raise ProcessingError(str(error)) from error
                except ProcessingError as error:
                    yield reporter.fail(error)
                    errors[f"source={source}"] = error
            else:
                if discovered_product_sources:
                    yield reporter.complete(f"Discovered products {' '.join(discovered_product_sources)}")
                else:
                    yield reporter.complete(f"No products discovered")

        for product_source in product_sources:
            reporter = JobReportBuilder(product_source)

            try:
                with self.StacTransaction(self).context(
                    message=f"Ingest {product_source} (processor={processor_id}:{processor.__version__})"
                ) as transaction:

                    yield reporter.progress("Identifying & versionning")

                    try:
                        try:
                            product_id = processor.id(product_source)
                            product_version = processor.version(product_source)
                        except Exception as error:
                            raise ProcessingError(str(error)) from error

                        yield reporter.progress(f"Identified {product_id} (version={product_version})")

                        head = next(self.commits)
                        cataloged_stac_object = head.search(product_id)

                        if cataloged_stac_object is not None:
                            try:
                                if product_version == _get_version(cataloged_stac_object):
                                    raise SkipIteration
                            except (_VersionNotFoundError, StacObjectError) as error:
                                yield reporter.progress(f"{product_id} found but unversionned, reprocessing")
                            else:
                                yield reporter.progress(f"Previous version of {product_id} found, reprocessing")
                        else:
                            yield reporter.progress(f"{product_id} not found, processing")

                        try:
                            processed_stac_object_file = processor.process(product_source)
                        except Exception as error:
                            raise ProcessingError(str(error)) from error

                        yield reporter.progress(f"Cataloging {product_id} (version={product_version})")

                        transaction.catalog(
                            processed_stac_object_file,
                            parent_id=parent_id,
                            catalog_assets=ingest_assets,
                            catalog_assets_out_of_scope=ingest_assets_out_of_scope,
                            catalog_out_of_scope=ingest_out_of_scope,
                            version=product_version
                        )

                        yield reporter.complete(f"Cataloged {product_id} (version={product_version})")
                    except SkipIteration:
                        yield reporter.complete(f"{product_id} (version={product_version}) is already cataloged with matching version, skipping")
                    except Exception as error:
                        yield reporter.fail(error)
                        raise error

            except Exception as error:
                errors[f"product={product_source}"] = error

        if errors:
            raise errors

    def prune(
        self,
        *product_ids: str,
        extract: Optional[str] = None
    ) -> Iterator[JobReport]:
        """Removes some product(s) from the catalog.

        Raises:
            ErrorGroup:
            UncatalogError:
            ExtractError:
        """
        errors = ErrorGroup()

        if extract is not None:
            extract = os.path.abspath(extract)

            try:
                os.makedirs(extract, exist_ok=True)

                if os.listdir(extract):
                    raise FileExistsError(f"{extract} is not empty.")
            except Exception as error:
                raise ExtractError(f"Couldn't create the extraction directory. {str(error)}") from error

        for product_id in product_ids:
            with self.StacTransaction(self).context(message=f"Prune : {product_id}") as transaction:
                reporter = JobReportBuilder(product_id)

                yield reporter.progress("Pruning")

                try:
                    yield reporter.progress("Uncataloging")

                    if extract is not None:
                        product_extract_dir = os.path.join(extract, product_id)
                    else:
                        product_extract_dir = None

                    try:
                        transaction.uncatalog(product_id, extract=product_extract_dir)
                    except FileNotFoundError:
                        yield reporter.complete("Not found in catalog")
                    else:
                        yield reporter.complete("Uncataloged")
                except Exception as error:
                    yield reporter.fail(error)

                    errors[product_id] = error

        if errors:
            if extract is not None:
                shutil.rmtree(extract, ignore_errors=True)

            raise errors
