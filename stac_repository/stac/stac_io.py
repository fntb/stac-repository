from __future__ import annotations

from typing import (
    Protocol,
    Any,
    Optional
)

from contextlib import (
    AbstractContextManager
)

import io
import os
import orjson
from urllib.parse import urlparse as _urlparse
import posixpath


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

    def get_asset(self, href: str) -> AbstractContextManager[io.RawIOBase | io.BufferedIOBase]:
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

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
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


class DefaultReadableStacIO(ReadableStacIO):
    """A default implementation of `ReadableStacIO` operating on the local filesystem."""

    def _assert_href_in_repository(self, href: str):
        if _urlparse(href, scheme="").scheme != "":
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        href = posixpath.normpath(posixpath.join(self._base_href, href))

        if not href.startswith(self._base_href):
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        return href

    def __init__(self, base_href: Optional[str] = None):
        self._base_href = posixpath.abspath(base_href) if base_href is not None else posixpath.abspath("/")

    def get(self, href: str) -> Any:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        with open(os_href, "r+b") as object_stream:
            try:
                return orjson.loads(object_stream.read())
            except orjson.JSONDecodeError as error:
                raise JSONObjectError from error

    def get_asset(self, href: str) -> AbstractContextManager[io.RawIOBase | io.BufferedIOBase]:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        return open(os_href, "r+b")


class DefaultStacIO(DefaultReadableStacIO, StacIO):
    """A default implementation of `StacIO` operating on the local filesystem."""

    def set(self, href: str, value: Any):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            os.remove(os_href)
        except FileNotFoundError:
            pass
