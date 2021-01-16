from typing import Dict

from pyspark.sql import DataFrame as SDF
from pyspark.sql import SparkSession

from psetl.extract import AbstractExtractor
from psetl.load import AbstractLoader
from psetl.transform import AbstractTransformer
from psetl.types import Config
from psetl.validation import AbstractValidator


class Pipeline:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def set_extract_results(self, outputs: Dict[str, SDF]) -> None:
        for config in self.config.inputs:
            outputs[config.name] = AbstractExtractor.from_config(
                config
            ).extract(config.uri)

    def set_transform_results(self, outputs: Dict[str, SDF]) -> None:
        if not self.config.transforms:
            return
        for config in self.config.transforms:
            inputs: Dict[str, SDF] = {}
            for k, v in config.inputs.items():
                inputs[k] = outputs[v]
            outputs[config.name] = AbstractTransformer.from_config(
                config
            ).transform(inputs)

    def validate(self, outputs: Dict[str, SDF]) -> None:
        if not self.config.validations:
            return
        for config in self.config.validations:
            AbstractValidator.from_config(config).validate(
                outputs[config.input]
            )

    def set_load_results(self, outputs: Dict[str, SDF]) -> None:
        for config in self.config.outputs:
            df = outputs[config.input]
            outputs[config.name] = AbstractLoader.from_config(config).load(
                df, config.uri
            )

    def run(self) -> Dict[str, SDF]:
        outputs: Dict[str, SDF] = {}
        self.set_extract_results(outputs)
        self.set_transform_results(outputs)
        self.validate(outputs)
        self.set_load_results(outputs)
        return outputs
