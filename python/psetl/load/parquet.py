from typing import Optional

from pyspark.sql import DataFrame as SDF

from psetl.load import AbstractLoader


class ParquetLoader(AbstractLoader):
    def __init__(
        self, num_partitions: Optional[int], mode: str = 'error'
    ) -> None:
        self.num_partitions = num_partitions
        self.mode = mode

    def load(self, df: SDF, uri: str) -> SDF:
        df_to_load = (
            df.repartition(self.num_partitions) if self.num_partitions else df
        )
        df_to_load.write.mode(self.mode).parquet(uri)

        return df_to_load
