from __future__ import annotations
from typing import Optional, Annotated
from os import PathLike
import tomllib

import pydantic


class Config(pydantic.BaseModel):
    repository: PathLike[str]
    catalog: CatalogConfig
    git: GitConfig


class CatalogConfig(pydantic.BaseModel):
    id: str
    title: Optional[str | None] = None
    description: Optional[str | None] = None


class GitConfig(pydantic.BaseModel):
    signature: Annotated[str, pydantic.StringConstraints(
        pattern="(.*)<(.*)>$")]
    lfs: Optional[GitLFSConfig] = None


class GitLFSConfig(pydantic.BaseModel):
    url: pydantic.AnyUrl
    filter: list[str]


def load_config(file: PathLike[str]) -> Config:
    with open(file, 'rb') as f:
        data = tomllib.load(f)
        return Config.model_validate(data)
