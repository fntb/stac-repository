
import pytest

import pystac

from stac_repository import BaseStacCommit
from stac_repository import StacCommit
from stac_repository import StacIndex

from .conftest import StacCommitDescription
from .conftest import StacRepositoryDescription


class TestStacCommit:

    @staticmethod
    def make_stac_commit(commit_description: StacCommitDescription) -> BaseStacCommit:
        if commit_description.is_commited:
            return StacCommit(
                commit_description.commit(),
                commit_description.catalog.get_self_href()
            )
        elif commit_description.is_staged:
            return StacIndex(
                commit_description.repository,
                commit_description.catalog.get_self_href()
            )
        else:
            raise ValueError(commit_description)

    def test_parents(self, stac_repository_description: StacRepositoryDescription):
        commit: StacIndex = self.make_stac_commit(
            stac_repository_description.index
        )
        expected_parent_commit: StacCommit = self.make_stac_commit(
            stac_repository_description.head
        )

        assert commit.parent.id == expected_parent_commit.id

        for i in range(len(stac_repository_description.commits) - 1):
            commit: StacCommit = self.make_stac_commit(
                stac_repository_description.commits[i + 1]
            )
            expected_parent_commit: StacCommit = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            assert commit.parent.id == expected_parent_commit.id

    def test_catalogs(self, stac_repository_description: StacRepositoryDescription):
        commit: StacIndex = self.make_stac_commit(
            stac_repository_description.index
        )

        assert commit.catalog.to_dict(
            include_self_link=False
        ) == stac_repository_description.index.catalog.to_dict(
            include_self_link=False
        )

        for i in range(len(stac_repository_description.commits)):
            commit: StacCommit = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            assert commit.catalog.to_dict(
                include_self_link=False
            ) == stac_repository_description.commits[i].catalog.to_dict(
                include_self_link=False
            )

    def test_fetch(self, stac_repository_description: StacRepositoryDescription):
        commit: StacIndex = self.make_stac_commit(
            stac_repository_description.index
        )

        commit.fetch(
            commit.catalog.get_self_href()
        )

        for i in range(len(stac_repository_description.commits)):
            commit: StacCommit = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            commit.fetch(
                commit.catalog.get_self_href()
            )

    def test_objects(self, stac_repository_description: StacRepositoryDescription):
        def get_object_ids(*objects: pystac.STACObject | tuple[pystac.STACObject, pystac.STACObject]):
            return set(
                stac_object.id if isinstance(
                    stac_object, pystac.STACObject) else stac_object[0].id
                for stac_object
                in objects
            )

        def test_commit_objects(commit: BaseStacCommit, commit_description: StacCommitDescription):
            assert get_object_ids(
                *commit.objects
            ) == commit_description.object_ids

            assert get_object_ids(
                *commit.added_objects
            ) == commit_description.added_object_ids

            assert get_object_ids(
                *commit.modified_objects
            ) == commit_description.modified_object_ids

            assert get_object_ids(
                *commit.removed_objects
            ) == commit_description.removed_object_ids

        commit: StacIndex = self.make_stac_commit(
            stac_repository_description.index)

        test_commit_objects(commit, stac_repository_description.index)

        for i in range(len(stac_repository_description.commits)):
            commit: StacCommit = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            test_commit_objects(commit, stac_repository_description.commits[i])
