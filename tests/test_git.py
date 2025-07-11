
import datetime

import pytest

from stac_repository.git.git import Repository as GitRepository

from .conftest import GitCommitDescription


class TestRepository():

    def test_is_not_init(self, dir):
        repository = GitRepository(dir)
        assert repository.is_init is False

    def test_is_init(self, empty_repository: GitRepository):
        assert empty_repository.is_init

    def test_init(self, empty_repository: GitRepository):
        repository = empty_repository
        pass

    def test_no_reinit(self, empty_repository: GitRepository):
        assert empty_repository.init() is False

    def test_init_has_not_head(self, empty_repository: GitRepository):
        assert empty_repository.head is None

    def test_initial_commit_has_head(self, single_commit_repository: GitCommitDescription):
        assert single_commit_repository.repository.head is not None

    @pytest.mark.skip(reason="Not Implemented")
    def test_commit_getter(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_refs(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_modified_files_from_index(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_modified_files_from_commit(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_reset_to_head(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_reset(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_cloning(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError

    @pytest.mark.skip(reason="Not Implemented")
    def test_pulling(self, full_repository: list[GitCommitDescription]):
        raise NotImplementedError


class TestCommit():

    def test_base_properties(self, single_commit_repository: GitCommitDescription):
        commit = single_commit_repository.repository.head

        assert datetime.datetime.now(
            datetime.timezone.utc) - datetime.timedelta(minutes=1) <= commit.datetime <= datetime.datetime.now(datetime.timezone.utc)
        assert commit.message == single_commit_repository.message

    @pytest.mark.skip(reason="Bad Testing Implementation")
    def test_refs(self, single_commit_repository: GitCommitDescription):
        commit = single_commit_repository.repository.head

        assert commit.refs == ["refs/heads/main"]

    def test_ancestors(self, single_commit_repository: GitCommitDescription):
        commit = single_commit_repository.repository.head

        assert commit.parent == None

    def test_modified_files(self, single_commit_repository: GitCommitDescription):
        commit = single_commit_repository.repository.head

        assert set(commit.modified_files) == set(single_commit_repository.added_files +
                                                 single_commit_repository.removed_files)
