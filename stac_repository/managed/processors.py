import importlib
import pkgutil

from .processor import Processor

discovered_processors: dict[str, Processor] = {
    name[len("stac_processor_"):]: importlib.import_module(name)
    for finder, name, ispkg
    in pkgutil.iter_modules()
    if name.startswith("stac_processor_")
}
