from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import List, Type, Union

from pyspark.sql import DataFrame as SDF

from psetl.types import InputConfig


class AbstractExtractor(metaclass=ABCMeta):
    @abstractmethod
    def extract(self, uri: Union[str, List[str]]) -> SDF:
        raise NotImplementedError

    @staticmethod
    def from_config(config: InputConfig) -> 'AbstractExtractor':
        mod_name, cls_name = config.extractor.rsplit('.', 1)
        mod = import_module(mod_name)
        kls: Type[AbstractExtractor] = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)  # type: ignore
        return kls()
