from os import path

import pytest

from stac_repository import StacTransaction
from stac_repository import StacTransactionCommitError
from stac_repository import StacRepository

from .conftest import StacRepositoryBuilder
from .conftest import StacRepositoryDescription
from .conftest import GitRepository


class TestStacTransaction:

    def test_stage_related_files(self, empty_repository: GitRepository):
        builder = StacRepositoryBuilder(empty_repository)

        repository = StacRepository(
            builder.repository.dir
        )

        stac_repository_description = builder.mutate(
            stage=False,
            commit=False
        ).log

        transaction = StacTransaction(
            repository
        )

        transaction.stage(
            stac_repository_description.working_directory.catalog.id
        )

        transaction.commit()

    def test_doesnt_stage_unrelated_files_and_raises_unclean_working_directory(self, empty_repository: GitRepository):
        builder = StacRepositoryBuilder(empty_repository)

        repository = StacRepository(
            builder.repository.dir
        )

        stac_repository_description = builder.mutate(
            stage=False,
            commit=False
        ).log

        open(
            path.join(
                stac_repository_description.working_directory.repository.dir,
                "unrelated_file.txt"
            ),
            "a"
        ).close()

        transaction = StacTransaction(
            repository
        )

        transaction.stage(
            stac_repository_description.working_directory.catalog.id
        )

        try:
            transaction.commit()
        except StacTransactionCommitError:
            pass
        else:
            raise AssertionError(
                "Should have raised StacTransactionCommitError")

    def test_context(self, empty_repository: GitRepository):
        builder = StacRepositoryBuilder(empty_repository)

        repository = StacRepository(
            builder.repository.dir
        )

        stac_repository_description = builder.mutate(
            stage=False,
            commit=False
        ).log

        with StacTransaction(
            repository
        ).context(
            message="test"
        ) as transaction:
            transaction.stage(
                stac_repository_description.working_directory.catalog.id
            )
