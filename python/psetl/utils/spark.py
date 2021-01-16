from typing import List, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SDF


def has_duplicate(df: SDF, cols: Union[str, List[str]]) -> bool:
    if isinstance(cols, str):
        return int(df.select(cols).count()) != int(
            df.select(cols).distinct().count()
        )

    return int(df.select(*cols).count()) != int(
        df.select(*cols).distinct().count()
    )


def has_null(df: SDF, cols: Union[str, List[str]]) -> bool:
    if isinstance(cols, str):
        return int(df.filter(F.col(cols).isNull()).count()) > 0

    return any([df.filter(F.col(c).isNull()).count() > 0 for c in cols])
