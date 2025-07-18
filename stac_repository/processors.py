from typing import (
    Dict,
    cast
)

import importlib
import pkgutil

from .processor import Processor
from .stac_processor import StacProcessor


discovered_processors: Dict[str, Processor] = {
    "stac": cast(Processor, StacProcessor),
    **{
        name[len("stac_processor_"):]: cast(Processor, importlib.import_module(name))
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("stac_processor_") and name != "stac_processor_cli"
    }
}
