from typing import List, Union

from pyspark.sql import DataFrame as SDF

from psetl.errors import DataFrameValidationError
from psetl.utils.spark import has_duplicate
from psetl.validation import AbstractValidator


class RecordNumberValidator(AbstractValidator):
    def __init__(self, num: int = 1) -> None:
        if num < 0:
            raise ValueError('num must be 0 or more')
        self.num = num

    def validate(self, df: SDF) -> bool:
        num_rec = df.count()
        if num_rec < self.num:
            raise DataFrameValidationError(
                f'number of records is required at least {self.num}, '
                f'dataframe has {num_rec}'
            )

        return True


class DuplicateValidator(AbstractValidator):
    def __init__(self, cols: Union[str, List[str]]) -> None:
        self.cols = cols

    def validate(self, df: SDF) -> bool:
        if has_duplicate(df, self.cols):
            raise DataFrameValidationError(
                f'duplicate value is contained at least one of {self.cols}'
            )

        return True
