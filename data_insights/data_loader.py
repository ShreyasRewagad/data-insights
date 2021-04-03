import os
from abc import ABC
from collections import Counter
from dataclasses import dataclass, field
from typing import List

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType

from data_insights.utils import get_spark_session


@dataclass(frozen=True, eq=True)
class DataLoader(ABC):
    """
    A data loader utility to load data from disk into a Spark.DataFrame object.
    The interface is meant to stream line data-loading process and also perform
    type casting. This is also meant to act as a column filter.

    :param file_path: Location of the data folder/file on disk.
    :param spark: An active spark session object.
    :param string_columns: A list of columns from the loaded data to be casted
        as string type.
    :param datetime_columns: A list of columns from the loaded data to be
        casted as timestamps in the UTC timezone. The UTC timezone is meant to
        help mitigate the timezone issues caused by having records with multiple
        timezones within the dataset.
    :param integer_columns: A list of columns from the loaded data to be casted
        as integer type.
    :param float_columns: A list of columns from the loaded data to be casted
        as float type.
    """

    # Initialization Arguments
    file_path: str = field(init=True, default=None)
    spark: SparkSession = field(init=True, default=get_spark_session(), hash=False)
    string_columns: List[str] = field(init=True, default_factory=lambda: list())
    datetime_columns: List[str] = field(init=True, default_factory=lambda: list())
    integer_columns: List[str] = field(init=True, default_factory=lambda: list())
    float_columns: List[str] = field(init=True, default_factory=lambda: list())

    # Inferred Arguments
    df: DataFrame = field(init=False, default=None, hash=False)
    columns: List[str] = field(init=False, default=None)

    def __post_init__(self):
        assert isinstance(self.file_path, str) and os.path.exists(self.file_path)
        assert isinstance(self.spark, SparkSession)
        assert os.path.splitext(self.file_path)[1].lower().strip() == ".csv"
        assert not any(
            _ > 1
            for _ in Counter(
                [
                    *self.string_columns,
                    *self.datetime_columns,
                    *self.integer_columns,
                    *self.float_columns,
                ],
            ).values()
        ), "No overlap must exist between string, datetime and numeric columns."
        object.__setattr__(self, "df", self._read_data())
        object.__setattr__(self, "columns", self.df.columns)

    def _read_data(self) -> DataFrame:
        """Loads the data and perform type casting."""
        _df = self.spark.read.csv(
            path=self.file_path, header="true", inferSchema="true"
        )
        for _cols in [
            self.string_columns,
            self.integer_columns,
            self.datetime_columns,
            self.float_columns,
        ]:
            if len(_cols) > 0:
                assert set(_cols).issubset(set(_df.columns)), (
                    "The following columns are missing from data available at "
                    f"{self.file_path} :\n"
                    f"{set(_cols).difference(set(_df.columns))}"
                )
        return _df.select(
            *[
                _fx(_col)
                for _cols, _fx in [
                    (
                        self.string_columns,
                        lambda _c: sf.col(_c).cast(StringType()).alias(_c),
                    ),
                    (
                        self.float_columns,
                        lambda _c: sf.col(_c).cast(FloatType()).alias(_c),
                    ),
                    (
                        self.integer_columns,
                        lambda _c: sf.col(_c).cast(IntegerType()).alias(_c),
                    ),
                    (
                        self.datetime_columns,
                        lambda _c: sf.to_utc_timestamp(sf.col(_c), tz="UTC").alias(_c),
                    ),
                ]
                for _col in _cols
            ]
        )
