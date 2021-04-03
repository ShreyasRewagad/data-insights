import inspect
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import AtomicType, StringType

from data_insights.data_loader import DataLoader
from data_insights.utils import get_spark_session, logger


class InsightLocation(Enum):
    """Enum for Location/Level of insights"""

    Invoice = 1
    Vendor = 2


class InsightType(Enum):
    """Enum for Type of insights"""

    VendorNotSeenInAWhile = 1
    AccrualAlert = 2
    LargeMonthIncreaseMtd = 3
    NoInvoiceReceived = 4


@dataclass(frozen=True, eq=True)
class Insight(ABC):
    """
    Base Insights interface to harden the implementation specifications for
    derived classes and create an extensible framework to enable easy addition
    of logic to uncover insights from dataset.

    :param spark: An active spark session object.
    """

    # Initialization Arguments
    spark: SparkSession = field(init=True, default=get_spark_session())

    # Inferred Arguments
    df: DataFrame = field(init=False, default=None)
    expected_columns: List[str] = field(init=False, default=None)
    insight_location: InsightLocation = field(init=False, default=None)
    insight_type: InsightType = field(init=False, default=None)

    def __post_init__(self):
        assert isinstance(self.insight_location, InsightLocation)
        assert isinstance(self.expected_columns, list)
        object.__setattr__(
            self, "insight_type", InsightType.__getitem__(self.__class__.__name__)
        )
        logger.info(f"Setting up Glean `{self.__class__.__name__}`:")
        logger.info(f"Type: `{self.insight_type.name} - {self.insight_type.value}`")
        logger.info(
            f"Location / Level: `{self.insight_location.name} - "
            f"{self.insight_location.value}`"
        )
        _expected_call_signature = inspect.FullArgSpec(
            args=["self"],
            varargs=None,
            varkw=None,
            defaults=None,
            kwonlyargs=[],
            kwonlydefaults=None,
            annotations={"return": DataFrame},
        )
        assert inspect.getfullargspec(self.__call__) == _expected_call_signature
        logger.info("All assertions passed!")
        logger.info("Evaluating the data transformations spark DAG.")
        _df = self.__call__()
        assert isinstance(_df, DataFrame)
        _missing_columns = (
            set(self.expected_columns)
            .union({"insight_text", "insight_date"})
            .difference(set(_df.columns))
        )
        assert len(_missing_columns) == 0, (
            "The following columns are missing from `data_insights`: "
            f"{_missing_columns}"
        )
        assert not {"insight_location", "insight_type"}.issubset(set(_df.columns))
        _df = self._add_uuid_column(df=_df, column_name="glean_id")
        _df = _df.select(
            "glean_id",
            sf.col("insight_date").alias("glean_date"),
            sf.col("insight_text").alias("glean_text"),
            sf.lit(self.insight_type.value).alias("glean_type"),
            sf.lit(self.insight_location.value).alias("glean_location"),
            *[
                sf.col(_col)
                for _col in set(self.expected_columns).difference(
                    {"insight_text", "insight_date"}
                )
            ],
        )
        object.__setattr__(self, "df", _df)
        del _df
        logger.info(
            f"Successfully created Glean `{self.__class__.__name__}` "
            f"DAG! Pending spark action."
        )

    @staticmethod
    def _check_column_types(
        data_loader: DataLoader, column_types: List[Tuple[str, AtomicType]]
    ):
        """Type checking on the loaded dataset."""
        assert isinstance(data_loader, DataLoader)
        assert all(isinstance(_c_t, tuple) and len(_c_t) == 2 for _c_t in column_types)
        assert all(
            isinstance(_col, str)
            and isinstance(_type, AtomicType)
            and _col in data_loader.columns
            and data_loader.df.schema.__getitem__(_col).dataType == _type
            for _col, _type in column_types
        )

    @staticmethod
    def _add_uuid_column(df: DataFrame, column_name: str) -> DataFrame:
        """Append a unique record identifier to help distinguish the triggered gleans."""
        assert isinstance(df, DataFrame)
        assert column_name not in df.columns
        return df.withColumn(
            column_name, sf.udf(f=lambda: str(uuid.uuid4()), returnType=StringType())()
        )

    @abstractmethod
    def __call__(self) -> DataFrame:
        raise NotImplementedError
