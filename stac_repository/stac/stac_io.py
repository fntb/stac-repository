from __future__ import annotations

from typing import (
    Protocol,
    Any,
    Optional,
    Iterator,
    BinaryIO,
)

from contextlib import contextmanager

from enum import (
    Flag,
    auto
)

import os
import posixpath
from urllib.parse import (
    urlparse as _urlparse,
    urljoin as _urljoin
)

import orjson
import requests


class JSONObjectError(ValueError):
    pass


class FileNotInRepositoryError(ValueError):
    pass


class ReadableStacIO(Protocol):

    _base_href: str
    """The base href under which hrefs can be retrieved under this implementation."""

    def get(self, href: str) -> Any:
        """Reads a JSON Object.

        This method is intended to retrieve STAC Objects, as such the returned 
        value should be a valid representation of a STAC Object, this validation 
        is not handled by the ReadableStacIO but downstream however.

        Raises:
            FileNotFoundError
            FileNotInRepositoryError
            JSONObjectError
        """
        ...

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        """Reads a binary Object.

        This method is intended to retrieve Asset files.

        This method must implement the ContextManager Protocol :

        ```
        with stac_io.get_asset(href) as asset_stream:
            ...
        ```

        Raises:
            FileNotFoundError
            FileNotInRepositoryError
        """
        ...


class StacIO(ReadableStacIO):

    def set(self, href: str, value: Any):
        """(Over)writes a JSON Object, which is a valid representation of a STAC Object.

        Raises:
            JSONObjectError
            FileNotInRepositoryError
        """
        ...

    def set_asset(self, href: str, value: BinaryIO):
        """(Over)writes a binary Object.

        This method is intended to save Asset files.

        Raises:
            FileNotInRepositoryError
        """
        ...

    def unset(self, href: str):
        """Deletes whatever object (if it exists) at `href`.

        Raises:
            FileNotInRepositoryError
        """
        ...


class DefaultReadableStacIOScope(Flag):
    BASE_STAC = 1
    """All STAC Objects under `base_href`"""

    BASE_ASSET = 2
    """All assets under `base_href`"""

    STAC = 1 | 4
    """All STAC Objects"""

    ASSET = 2 | 8
    """All assets"""


def _is_file_href(self, href: str) -> bool:
    return _urlparse(href, scheme="").scheme == ""


class DefaultReadableStacIO(ReadableStacIO):
    """A default implementation of `ReadableStacIO` operating on the local filesystem."""

    # _scope: DefaultReadableStacIOScope = DefaultReadableStacIOScope.BASE_STAC | DefaultReadableStacIOScope.BASE_ASSET

    # @property
    # def _is_base_file_href(self) -> bool:
    #     return self._is_file_href(self._base_href)

    # def _is_file_href(self, href: str) -> bool:
    #     return _urlparse(href, scheme="").scheme == ""

    # def _abs_href(self, href: str) -> str:
    #     if self._is_base_file_href:
    #         if self._is_file_href(href):
    #             return posixpath.normpath(posixpath.join(self._base_href, href))
    #         else:
    #             return href
    #     else:
    #         if self._is_file_href(href):
    #             return _urljoin(self._base_href, href)
    #         else:
    #             return href

    # def _assert_stac_object_within_base(self, abs_href: str) -> None:
    #     if not abs_href.startswith(self._base_href):
    #         raise FileNotInRepositoryError(f"{abs_href} is not in repository {self._base_href}")

    # def _assert_stac_object_within_readable_scope(self, abs_href: str) -> None:
    #     if DefaultReadableStacIOScope.STAC in self._scope:
    #         return
    #     elif DefaultReadableStacIOScope.BASE_STAC in self._scope:
    #         return self._assert_stac_object_within_base(abs_href)
    #     else:
    #         raise FileNotInRepositoryError(f"{abs_href} cannot be read, no readable scope defined")

    # def _assert_asset_within_base(self, abs_href: str) -> None:
    #     if not abs_href.startswith(self._base_href):
    #         raise FileNotInRepositoryError(f"{abs_href} is not in repository {self._base_href}")

    # def _assert_asset_within_readable_scope(self, abs_href: str) -> None:
    #     if DefaultReadableStacIOScope.ASSET in self._scope:
    #         return
    #     elif DefaultReadableStacIOScope.BASE_ASSET in self._scope:
    #         return self._assert_asset_within_base(abs_href)
    #     else:
    #         raise FileNotInRepositoryError(f"{abs_href} cannot be read, no readable scope defined")

    def __init__(
        self,
        # base_href: str,
        # scope: DefaultReadableStacIOScope = DefaultReadableStacIOScope.BASE_STAC | DefaultReadableStacIOScope.BASE_ASSET
    ):
        # if self._is_file_href(base_href):
        #     self._base_href = posixpath.abspath(base_href)
        # else:
        #     self._base_href = base_href

        # self._scope = scope
        pass

    def get(self, href: str) -> Any:
        # href = self._abs_href(href)

        # self._assert_stac_object_within_readable_scope(href)

        if _is_file_href(href):
            os_href = os.path.abspath(href)

            with open(os_href, "r+b") as object_stream:
                try:
                    return orjson.loads(object_stream.read())
                except orjson.JSONDecodeError as error:
                    raise JSONObjectError from error
        else:
            response = requests.get(href)

            if response.status_code == 404:
                raise FileNotFoundError(f"{href} does not exist")
            else:
                response.raise_for_status()

            try:
                return response.json()
            except requests.JSONDecodeError as error:
                raise JSONObjectError from error

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        # href = self._abs_href(href)

        # self._assert_asset_within_readable_scope(href)

        if _is_file_href(href):
            os_href = os.path.abspath(href)

            with open(os_href, "r+b") as asset_stream:
                yield asset_stream
        else:
            response = requests.get(href, stream=True)

            yield response.raw


class DefaultStacIO(DefaultReadableStacIO, StacIO):
    """A default implementation of `StacIO` operating on the local filesystem."""

    def set(self, href: str, value: Any):
        # href = self._abs_href(href)

        # self._assert_stac_object_within_base(href)

        if not _is_file_href(href):
            raise FileNotInRepositoryError(f"Cannot write {href} - this is not a local file uri")

        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    def set_asset(self, href: str, value: BinaryIO):
        # href = self._abs_href(href)

        # self._assert_asset_within_base(href)

        if not _is_file_href(href):
            raise FileNotInRepositoryError(f"Cannot write {href} - this is not a local file uri")

        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        # href = self._abs_href(href)

        # self._assert_asset_within_base(href)

        if not _is_file_href(href):
            raise FileNotInRepositoryError(f"Cannot write {href} - this is not a local file uri")

        os_href = os.path.abspath(href)

        try:
            os.remove(os_href)
        except FileNotFoundError:
            pass
