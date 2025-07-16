from __future__ import annotations
from typing import (
    Optional
)
import os
from os import path, PathLike
import subprocess
from typing import NamedTuple
import datetime
import re
from functools import cache, cached_property
import urllib
import urllib.parse
import logging
import abc
import io
import contextlib
import tempfile

from ..__about__ import __version__, __name_public__
from .cache import CacheMeta

_logger = logging.getLogger(f"{__name_public__}:git")


class GitError(Exception):

    _code: int

    @property
    def code(self):
        return self._code

    def __init__(self, *args: object, code: int = 1):
        super().__init__(*args)
        self._code = code

    @staticmethod
    def is_file_not_found_error(error: GitError) -> bool:
        return re.search(r"path.+does not exist", str(error)) or re.search(r"pathspec.+did not match any files", str(error))


class IllegalReCloneError(Exception):
    pass


class UnsupportedCloneRemoteError(ValueError):
    pass


class RefNotFoundError(ValueError):
    pass


class SignatureError(ValueError):
    pass


class Signature(NamedTuple):

    @staticmethod
    @cache
    def make(signature_str: str) -> Signature:
        result = re.fullmatch("(.*)<(.*)>$", signature_str)

        if result is None:
            raise SignatureError(
                f"Invalid git signature string {signature_str}")

        name = result.group(1).strip()
        email = result.group(2).strip()

        return Signature(name=name, email=email)

    name: str
    email: str = ""

    def __str__(self) -> str:
        return f"{self.name} <{self.email}>"


class Commit(metaclass=CacheMeta):

    _id: str
    _repository: Repository

    def __init__(self, repository: Repository, id: str):
        self._repository = repository
        self._id = id

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def id(self) -> str:
        return self._id

    @property
    def refs(self) -> list[str]:
        result = self._repository._git("show-ref")

        return list(
            map(
                lambda line: line[(len(self._id) + 1):],
                filter(
                    lambda line: line.startswith(self._id),
                    result.stdout.splitlines()
                )
            )
        )

    @cached_property
    def committer(self) -> Signature:
        result = self._repository._git(
            "show",
            "-s",
            r"--format=%cn <%ce>",
            self._id
        )

        return Signature.make(result.stdout.strip())

    @cached_property
    def author(self) -> Signature:
        result = self._repository._git(
            "show",
            "-s",
            r"--format=%an <%ae>",
            self._id
        )

        return Signature.make(result.stdout.strip())

    @cached_property
    def datetime(self) -> datetime.datetime:
        result = self._repository._git(
            "show",
            "-s",
            r"--format=%ct",
            self._id
        )

        return datetime.datetime.fromtimestamp(
            float(result.stdout.strip()),
            datetime.timezone.utc
        )

    @cached_property
    def message(self) -> str:
        result = self._repository._git(
            "show",
            "-s",
            r"--format=%B",
            self._id
        )

        return result.stdout.strip()

    @cached_property
    def parent(self) -> Commit | None:
        result = self._repository._git(
            "rev-list",
            self._id
        )

        parent_ids = result.stdout.strip().splitlines()[1:]

        return Commit(self._repository, parent_ids[0]) if parent_ids else None

    def tag(self, tag: str, message: str | None = None):
        self._repository._git(
            "tag",
            "-a",
            tag,
            "-m",
            message or tag,
            self.id
        )

    def smudge(self, file: str) -> io.BytesIO:
        if self._repository.is_lfs_installed:
            pointer = self.read(file)

            result = self._repository._git(
                "lfs",
                "smudge",
                text=False,
                input=pointer.encode("utf-8")
            )

            return io.BytesIO(result.stdout)
        else:
            return self.read(file, text=False)

    def read(self, file: str, text: bool = True) -> str:
        file_rel = path.relpath(file, self._repository.dir)

        result = self._repository._git(
            "show",
            f"{self._id}:{file_rel}",
            text=text
        )

        return result.stdout

    @cache
    def list_modified(self) -> list[PathLike[str]]:
        result = self._repository._git(
            "show",
            "--format=",
            "--name-only",
            self._id
        )

        return [path.join(self._repository.dir, file_name) for file_name in result.stdout.strip().splitlines()]

    @cached_property
    def modified_files(self) -> list[PathLike[str]]:
        return self.list_modified()


class BaseRepository():
    _dir: PathLike[str]

    @property
    def is_lfs_installed(self) -> bool:
        try:
            self._git("lfs", "version")
        except GitError:
            return False
        else:
            return True

    def __init__(self, dir: PathLike[str]) -> None:
        self._dir = path.abspath(dir)

    @property
    def dir(self):
        return self._dir

    def _git(
            self,
            *args: str,
            env: dict[str, str] | None = None,
            cwd: str | None = None,
            input: str | None = None,
            text: bool = True
    ) -> subprocess.CompletedProcess[str]:

        _logger.debug("git " + " ".join(args))

        result = subprocess.run(
            [
                "git",
                *args
            ],
            cwd=cwd or self.dir,
            capture_output=True,
            text=text,
            env=env,
            input=input
        )

        if result.returncode != 0:
            if text:
                _logger.debug("\n" + result.stderr)
            raise GitError(result.stderr, code=result.returncode)

        if text:
            _logger.debug("\n" + result.stdout)

        return result

    @property
    def refs(self) -> list[str]:
        result = self._git("show-ref")

        return list(
            map(
                lambda line: line.split(" ")[1],
                result.stdout.splitlines()
            )
        )

    def get_commit(self, ref: str) -> Commit | None:
        try:
            result = self._git(
                "rev-parse",
                ref
            )

            return Commit(self, result.stdout.strip())
        except GitError:
            return None

    def __getitem__(self, ref: str) -> Commit | None:
        return self.get_commit(ref)

    @property
    def head(self) -> Commit | None:
        return self.get_commit("HEAD")


class BareRepository(BaseRepository):

    @property
    def is_init(self) -> bool:
        try:
            result = self._git(
                "rev-parse",
                "--git-dir"
            )

            return result.stdout.strip() == "."
        except GitError:
            return False
        except FileNotFoundError:
            return False

    def init(self) -> bool:
        if self.is_init:
            return False

        self._git("init", "--bare")

        return True

    @contextlib.contextmanager
    def tempclone(self):
        with tempfile.TemporaryDirectory() as dir:
            repository = Repository(dir)
            repository.clone(self.dir, fetch_lfs_files=False)

            yield repository

            repository.push()

    def clone(self, dir: Optional[str] = None):
        if dir is None:
            dir = tempfile.mkdtemp()

        repository = Repository(dir)
        repository.clone(self.dir, fetch_lfs_files=False)

        return repository


class Repository(BaseRepository):

    @property
    def is_init(self) -> bool:
        try:
            result = self._git(
                "rev-parse",
                "--git-dir"
            )

            return result.stdout.strip() == ".git"
        except GitError:
            return False
        except FileNotFoundError:
            return False

    def init(self) -> bool:
        if self.is_init:
            return False

        self._git("init")

        if self.is_lfs_installed:
            self._git("lfs", "install")

        return True

    def lfs_track(self, file: str):
        if self.is_lfs_installed:
            self._git(
                "lfs",
                "track",
                path.relpath(file, self.dir)
            )

    @property
    def lfs_url(self) -> str | None:
        if self.is_lfs_installed:
            try:
                lfs_url = self._git(
                    "config",
                    "-f",
                    ".lfsconfig",
                    "--get",
                    "lfs.url"
                )
            except GitError as error:
                if str(error).strip() == "":
                    return None
                else:
                    raise error

            if lfs_url.stdout.strip() == "":
                return None
            else:
                return lfs_url.stdout.strip()

    @lfs_url.setter
    def lfs_url(self, value: str | None):
        if self.is_lfs_installed:
            if value is None:
                self._git(
                    "config",
                    "-f",
                    ".lfsconfig",
                    "--unset",
                    "lfs.url"
                )
            else:
                self._git(
                    "config",
                    "-f",
                    ".lfsconfig",
                    "lfs.url",
                    f"{value}"
                )

    def add(self, *added_files: PathLike[str]):
        self._git(
            "add",
            *[path.relpath(modified_file, self.dir) for modified_file in added_files]
        )

    def stage_lfs(self):
        if self.is_lfs_installed:
            if os.path.exists(os.path.join(self._dir, ".lfsconfig")):
                self._git(
                    "add",
                    ".lfsconfig",
                )
            else:
                try:
                    self._git(
                        "rm",
                        ".lfsconfig",
                    )
                except GitError as error:
                    if GitError.is_file_not_found_error(error):
                        pass
                    else:
                        raise error

            if os.path.exists(os.path.join(self._dir, ".gitattributes")):
                self._git(
                    "add",
                    ".gitattributes",
                )
            else:
                try:
                    self._git(
                        "rm",
                        ".gitattributes",
                    )
                except GitError as error:
                    if GitError.is_file_not_found_error(error):
                        pass
                    else:
                        raise error

    def remove(self, *removed_files: PathLike[str]):
        self._git(
            "rm",
            *[path.relpath(modified_file, self.dir) for modified_file in removed_files]
        )

    def stage_all(self):
        self._git(
            "add",
            "--all"
        )

    def commit(
        self,
        message: str,
    ) -> Commit:

        signature = Signature.make(f"{__name_public__}:{__version__} <>")

        self._git(
            "commit",
            "--allow-empty",
            "-m",
            message,
            env={
                **dict(os.environ),
                "GIT_AUTHOR_NAME": signature.name,
                "GIT_AUTHOR_EMAIL": signature.email,
                "GIT_COMMITTER_NAME": signature.name,
                "GIT_COMMITTER_EMAIL": signature.email,
                "GIT_COMMITTER_DATE": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
        )

        return self.head

    def clone(self, origin_url: str, fetch_lfs_files: bool = True):
        if self.is_init:
            raise IllegalReCloneError

        mode = urllib.parse.urlparse(origin_url, scheme="file")

        if mode.scheme == "file":
            self._git(
                "clone",
                origin_url,
                path.split(self.dir)[1],
                cwd=path.split(self.dir)[0]
            )
        elif mode.scheme == "ssh":
            self._git(
                "clone",
                origin_url,
                path.split(self.dir)[1],
                cwd=path.split(self.dir)[0]
            )
        else:
            raise UnsupportedCloneRemoteError(origin_url)

        if self.is_lfs_installed:
            self._git("lfs", "install")

            if fetch_lfs_files:
                self._git(
                    "lfs",
                    "fetch",
                    "--all"
                )

    def pull(self, fetch_lfs_files: bool = True):
        self._git(
            "pull"
        )

        if self.is_lfs_installed and fetch_lfs_files:
            self._git(
                "lfs",
                "fetch",
                "--all"
            )

    def push(self):
        self._git(
            "push"
        )

    def reset(self, ref: str = "HEAD", clean_modified_files: bool = False):
        commit = self[ref]
        if commit is None:
            raise RefNotFoundError

        self._git(
            "reset",
            "--hard",
            commit.id
        )

        if clean_modified_files:
            self.clean()

    def clean(self):
        self._git(
            "clean",
            "-f",
            "-d"
        )

    def smudge(self, file: str) -> io.BytesIO:
        if self.is_lfs_installed:
            pointer = self.read(file)

            result = self._git(
                "lfs",
                "smudge",
                text=False,
                input=pointer.encode("utf-8")
            )

            return io.BytesIO(result.stdout)
        else:
            return self.read(file, text=False)

    def read(self, file: str, text: bool = True) -> str:
        file_rel = path.relpath(file, self.dir)

        result = self._git(
            "show",
            f":{file_rel}",
            text=text
        )

        return result.stdout

    def list_modified(self) -> list[PathLike[str]]:
        result = self._git(
            "ls-files",
            "--others",
            "--exclude-standard"
        )

        return [path.join(self.dir, file_name) for file_name in result.stdout.strip().splitlines()]

    @property
    def modified_files(self) -> list[PathLike[str]]:
        return self.list_modified()


class AbstractTagStrategy(abc.ABC):

    @abc.abstractmethod
    def make(repository: Repository) -> str:
        raise NotImplementedError


class IncrementalTagStrategy(AbstractTagStrategy):

    _tag: str

    def __init__(self, tag: str) -> None:
        self._tag = tag

    def make(self, repository: Repository):

        n = len(
            filter(
                lambda ref: ref.startswith(self._tag),
                repository.refs
            )
        )

        tag = f"{self._tag}-{n}"

        return tag
