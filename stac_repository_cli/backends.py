import importlib
import pkgutil

from stac_repository.backend import Backend

import stac_repository.file as file_backend
import stac_repository.git as git_backend

discovered_backends: dict[str, Backend] = {
    **{
        name[len("stac_repository_backend_"):]: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("stac_repository_backend_")
    },
    "file": file_backend,
    "git": git_backend
}
