import importlib
import pkgutil

from .processor import Processor
from .none_processor import NoneProcessor


discovered_processors: dict[str, Processor] = {
    "none": NoneProcessor,
    **{
        name[len("stac_processor_"):]: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("stac_processor_")
    }
}
