import importlib
import pkgutil

from .processor import Processor
from .stac_processor import StacProcessor


discovered_processors: dict[str, Processor] = {
    "stac": StacProcessor,
    **{
        name[len("stac_processor_"):]: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("stac_processor_") and name != "stac_processor_cli"
    }
}
