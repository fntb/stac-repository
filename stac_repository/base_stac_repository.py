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


class ConfigError(ValueError):
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


_Self = TypeVar("_Self", bound="BaseStacRepository")


class BaseStacRepository(metaclass=ABCMeta):

    StacConfig: Optional[Type[BaseModel]] = None
    StacTransaction: Type[BaseStacTransaction] = BaseStacTransaction
    StacCommit: Type[BaseStacCommit] = BaseStacCommit

    @classmethod
    def validate_config(
        cls,
        config: Optional[Dict[str, str]] = None
    ) -> Optional[BaseModel]:
        if cls.StacConfig is not None and config is not None:
            try:
                return cls.StacConfig.model_validate(config)
            except ValidationError as error:
                raise ConfigError("Invalid configuration") from error

    @classmethod
    def validate_config_option(
        cls,
        config_key: str,
        config_value: Optional[str] = None
    ) -> Any:
        if cls.StacConfig is None:
            raise ConfigError(f"Configuration not available")

        if config_key not in cls.StacConfig.model_fields.keys():
            raise ConfigError(f"No such configuration option \"{config_key}\"")

        if config_value is None:
            return None

        option_adapter = TypeAdapter(cls.StacConfig.model_fields[config_key].annotation)

        try:
            return option_adapter.validate_python(config_value)
        except ValidationError as error:
            raise ConfigError("Invalid \"{key}\" configuration option value") from error

    @classmethod
    @abstractmethod
    def init(
        cls: Type[_Self],
        repository: str,
        root_catalog: Catalog,
        config: Optional[Dict[str, str]] = None
    ) -> _Self:
        """Create a new repository.

        Raises:
            RepositoryAlreadyInitializedError: If the repository already exists.
        """
        raise NotImplementedError

    @abstractmethod
    def __init__(
        self,
        repository: str,
    ):
        """Open an existing repository.

        Raises:
            RepositoryNotFoundError: If the repository does not exist.
        """
        raise NotImplementedError

    def get_config(self) -> BaseModel:
        raise ConfigError("Configuration not available")

    def set_config(
        self,
        config_key: str,
        config_value: str
    ) -> None:
        raise ConfigError("Configuration not available")

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

    def _ingest_product(
        self,
        transaction: BaseStacTransaction,
        processor: Processor,
        product_source: str,
        parent_id: Optional[str] = None,
        ingest_assets: bool = False,
        ingest_assets_out_of_scope: bool = False,
        ingest_out_of_scope: bool = False,
    ) -> Iterator[JobReport]:
        """Ingests a product.

        Raises:
            ProcessingError: Processor raised an error
            FileNotFoundError: Product source does not exist
            StacObjectError: Product source is not a valid STAC object
            HrefError:  Product source href (scheme) cannot be processed
            UncatalogError:
                - Old product version couldn't be deleted sucessfully
                - Cannot uncatalog the root
            CatalogError:
        """
        reporter = JobReportBuilder(product_source)

        yield reporter.progress("Identifying & versionning")

        try:
            try:
                product_id = processor.id(product_source)
                product_version = processor.version(product_source)
            except Exception as error:
                raise ProcessingError(str(error)) from error

            head = next(self.commits)
            cataloged_stac_object = head.search(product_id)

            if cataloged_stac_object is not None:
                try:
                    if product_version == _get_version(cataloged_stac_object):
                        raise SkipIteration
                except (_VersionNotFoundError, StacObjectError) as error:
                    yield reporter.progress("Product found but unversionned, reprocessing")
                else:
                    yield reporter.progress("Previous version of the product found, reprocessing")
            else:
                yield reporter.progress("Product not found, processing")

            try:
                processed_stac_object_file = processor.process(product_source)
            except Exception as error:
                raise ProcessingError(str(error)) from error

            yield reporter.progress("Cataloging")

            transaction.catalog(
                processed_stac_object_file,
                parent_id=parent_id,
                catalog_assets=ingest_assets,
                catalog_assets_out_of_scope=ingest_assets_out_of_scope,
                catalog_out_of_scope=ingest_out_of_scope
            )

            yield reporter.complete("Cataloged")
        except SkipIteration:
            yield reporter.complete("Product is already cataloged with matching version, skipping")
        except Exception as error:
            yield reporter.fail(error)
            raise error

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

        with self.StacTransaction(self).context(
            message=f"Ingest (processor={processor_id}:{processor.__version__}) : \n\n - " +
            "\n - ".join(product_sources)
        ) as transaction:
            for product_source in product_sources:
                try:
                    yield from self._ingest_product(
                        processor=processor,
                        product_source=product_source,
                        parent_id=parent_id,
                        transaction=transaction,
                        ingest_assets=ingest_assets,
                        ingest_assets_out_of_scope=ingest_assets_out_of_scope,
                        ingest_out_of_scope=ingest_out_of_scope,
                    )
                except Exception as error:
                    errors[f"product={product_source}"] = error

            if errors:
                raise errors

    def prune(
        self,
        *product_ids: str,
    ) -> Iterator[JobReport]:
        """Removes some product(s) from the catalog.

        Raises:
            ErrorGroup:
            UncatalogError:
        """
        errors = ErrorGroup()

        transaction_message = "Prune : \n\n - " + "\n - ".join(product_ids)
        with self.StacTransaction(self).context(message=transaction_message) as transaction:
            for product_id in product_ids:
                reporter = JobReportBuilder(product_id)

                yield reporter.progress("Pruning")

                try:
                    yield reporter.progress("Uncataloging")

                    try:
                        transaction.uncatalog(product_id)
                    except FileNotFoundError:
                        yield reporter.complete("Not found in catalog")
                    else:
                        yield reporter.complete("Uncataloged")
                except Exception as error:
                    yield reporter.fail(error)

                    errors[product_id] = error

            if errors:
                raise errors
