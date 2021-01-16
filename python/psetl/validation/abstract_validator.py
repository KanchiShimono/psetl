from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Type

from pyspark.sql import DataFrame as SDF

from psetl.types import ValidationConfig


class AbstractValidator(metaclass=ABCMeta):
    @abstractmethod
    def validate(self, df: SDF) -> bool:
        raise NotImplementedError

    @staticmethod
    def from_config(config: ValidationConfig) -> 'AbstractValidator':
        mod_name, cls_name = config.validator.rsplit('.', 1)
        mod = import_module(mod_name)
        kls: Type[AbstractValidator] = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)  # type: ignore
        return kls()
