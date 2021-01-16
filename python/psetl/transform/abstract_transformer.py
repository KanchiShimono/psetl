from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Dict, Type

from pyspark.sql import DataFrame as SDF

from psetl.types import TransformConfig


class AbstractTransformer(metaclass=ABCMeta):
    @abstractmethod
    def transform(self, inputs: Dict[str, SDF]) -> SDF:
        raise NotImplementedError

    @staticmethod
    def from_config(config: TransformConfig) -> 'AbstractTransformer':
        mod_name, cls_name = config.transformer.rsplit('.', 1)
        mod = import_module(mod_name)
        kls: Type[AbstractTransformer] = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)  # type: ignore
        return kls()
