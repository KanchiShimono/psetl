from typing import List, Union

from pyspark.sql import DataFrame as SDF
from pyspark.sql import SparkSession

from psetl.extract import AbstractExtractor


class ParquetExtractor(AbstractExtractor):
    @property
    def spark(self) -> SparkSession:
        if not hasattr(self, '_spark'):
            self._spark = SparkSession.builder.getOrCreate()
            return self._spark
        if not isinstance(self._spark, SparkSession):
            self._spark = SparkSession.builder.getOrCreate()
            return self._spark
        return self._spark

    def extract(self, uri: Union[str, List[str]]) -> SDF:
        if isinstance(uri, list):
            return self.spark.read.parquet(*uri)
        return self.spark.read.parquet(uri)
