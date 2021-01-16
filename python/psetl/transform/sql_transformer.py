from typing import Dict

from pyspark.sql import DataFrame as SDF
from pyspark.sql import SparkSession

from psetl.transform import AbstractTransformer


class SqlTransformer(AbstractTransformer):
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.spark = SparkSession.builder.getOrCreate()

    def transform(self, inputs: Dict[str, SDF]) -> SDF:
        for k, v in inputs.items():
            v.createOrReplaceTempView(k)
        return self.spark.sql(self.sql)
