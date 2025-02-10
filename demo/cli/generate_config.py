from os import PathLike
from os import path
import tempfile
import tomllib

from stac_repository_cli import Config
from stac_repository_cli import CatalogConfig
from stac_repository_cli import GitConfig


def generate_config() -> PathLike[str]:
    dir = tempfile.mkdtemp(
        prefix="stac_repository-demo-"
    )

    config = Config(
        repository=dir,
        catalog=CatalogConfig(
            id="root"
        ),
        git=GitConfig(
            signature="Demo <demo@uca.fr>"
        )
    )

    config_toml = """repository = "{repository}"\n\n[catalog]\nid = "{id}"\n\n[git]\nsignature = "{signature}"\n""".format(
        repository=dir,
        id=config.catalog.id,
        signature=config.git.signature,
    )

    (_, config_file) = tempfile.mkstemp(
        prefix="stac_repository-demo-",
        suffix=".toml"
    )

    with open(config_file, "w") as config_file_pipe:
        config_file_pipe.write(config_toml)

    return config_file
