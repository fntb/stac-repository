import tomllib
from os import path

import pytest

from stac_repository.__about__ import __version__, __name_public__


@pytest.fixture
def pyproject():
    pyproject_file = path.normpath(
        path.join(path.dirname(__file__), "..", "pyproject.toml"))
    with open(pyproject_file, "rb") as f:
        pyproject = tomllib.load(f)
    return pyproject


def test_versions_do_not_diverge(pyproject):
    assert pyproject["project"]["version"] == __version__


def test_names_match(pyproject):
    assert pyproject["project"]["name"] == __name_public__
