from __future__ import annotations

from typing import (
    Dict,
    Optional,
    List
)

import os

from pydantic import (
    BaseModel,
    ValidationError,
    model_validator,
    ConfigDict
)

from tomlkit import (
    parse as load_toml,
    dumps as dump_toml
)

from .backends import discovered_backends


def join_str(l: List[str]) -> str:
    return '"' + '" "'.join(l) + '"'


class RepositoryConfig(BaseModel):
    backend: str
    active: bool = False

    model_config = ConfigDict(
        extra="allow",
    )

    def get_backend(self):
        backend = discovered_backends.get(self.backend, None)

        if backend is None:
            raise ValueError(f"Backend \"{self.backend}\" not found")
        else:
            return backend

    def get_backend_config(self):
        backend = self.get_backend()

        if backend.StacRepository.StacConfig is None:
            if self.model_extra != {}:
                raise ValueError(f"Options {join_str(self.model_extra.keys())} not supported")
            else:
                return None
        else:
            return backend.StacRepository.StacConfig.model_validate(self.model_extra)

    @model_validator(mode="after")
    def validate_backend_config(self):
        self.get_backend_config()

        return self


class RepositoriesConfig(Dict[str, RepositoryConfig]):

    _file: str

    @classmethod
    def load(cls, file: str = os.path.join(os.path.expanduser("~"), ".stac-repository.toml")) -> RepositoriesConfig:

        try:
            with open(file, "r") as config_stream:
                config_str = config_stream.read()
        except FileNotFoundError:
            return cls(file=file)
        else:
            try:
                config_dict = load_toml(config_str)
            except Exception as error:
                raise ValueError(f"Bad TOML ({file}). {str(error)}")

            if not isinstance(config_dict, Dict):
                raise ValueError(f"Config should be a dictionnary ({file})")

            config: Dict[str, RepositoryConfig] = {}
            active_repositories: List[str] = []

            for (repository_id, repository_config_dict) in config_dict.items():
                try:
                    repository_config = RepositoryConfig.model_validate(repository_config_dict)
                except (ValidationError, ValueError) as error:
                    raise ValueError(f"\"{repository_id}\" config error. {str(error)} ({file})") from error
                else:
                    config[repository_id] = repository_config
                    if repository_config.active:
                        active_repositories.append(repository_id)

            if len(active_repositories) > 1:
                raise ValueError(
                    f"Multiple active repositories found {join_str(active_repositories)} ({file})"
                )

            return cls(file=file, **config)

    def __init__(self, *args, file: str, **kwargs: RepositoryConfig):
        self._file = file
        self.update(*args, **kwargs)

    @property
    def active(self) -> Optional[RepositoryConfig]:
        for repository_config in self.values():
            if repository_config.active:
                return repository_config

    @active.setter
    def active(self, repository_id: Optional[str] = None):
        if repository_id is None:
            if (current_default := self.active) is not None:
                current_default.active = False
        else:
            if repository_id not in self:
                raise ValueError(f"Repository {repository_id} not found")

            if (current_default := self.active) is not None:
                current_default.active = False

            self[repository_id].active = True

    def save(self):
        with open(self._file, "w") as config_stream:
            config_stream.write(dump_toml({
                repository_id: repository_config.model_dump()
                for (repository_id, repository_config) in self.items()
            }))
