import os
from dataclasses import dataclass, field
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession

from data_insights.insights import Insight
from data_insights.utils import get_spark_session, logger


@dataclass(frozen=True, eq=True)
class Pipeline:
    """
    Executes the `gleans` specified and consolidates the output into a single
    spark.DataFrame object and dumps the output to the specified `path`.

    :param spark: An active spark session object.
    :param gleans: A list of object of class `Insights`.
    :param path: The path on the disk where the consolidated output for the
        gleans is to be store.
    """

    # Initialization Arguments
    spark: SparkSession = field(init=True, default=get_spark_session())
    gleans: List[Insight] = field(init=True, default=None)
    path: str = field(init=True, default=None)

    # Inferred Arguments
    df: DataFrame = field(init=True, default=None)

    def __post_init__(self):
        assert isinstance(self.spark, SparkSession)
        logger.info("Inspecting Glean DataFrame(s)")
        _mandatory_columns = [
            "glean_id",
            "glean_date",
            "glean_text",
            "glean_type",
            "glean_location",
        ]
        _expected_columns = [
            "invoice_id",
            "canonical_vendor_id",
        ]
        assert isinstance(self.gleans, list) and all(
            isinstance(_glean, Insight)
            and set(_mandatory_columns).issubset(set(_glean.df.columns))
            for _glean in self.gleans
        )
        assert self.path is not None
        assert os.path.splitext(self.path)[1].lower().strip() == ".csv"
        if not os.path.exists(os.path.abspath(os.path.dirname(self.path))):
            os.makedirs(name=os.path.abspath(os.path.dirname(self.path)))
            logger.info(
                f"Creating directory: `{os.path.abspath(os.path.dirname(self.path))}`"
            )

        logger.info("Combining DataFrame(s) from multiple Glean(s).")
        _df = reduce(
            lambda _l, _r: _l.unionByName(_r, allowMissingColumns=True),
            [_glean.df for _glean in self.gleans],
        )
        assert (
            set(_expected_columns).union(set(_mandatory_columns)).issubset(_df.columns)
        )
        _df.select(*_mandatory_columns, *_expected_columns).write.mode("overwrite").csv(
            path=self.path, header=True
        )
        logger.info(f"Output written out to {self.path}")
        _df = self.gleans[0].spark.read.csv(
            path=self.path, header=True, inferSchema=True
        )
        object.__setattr__(self, "df", _df)
        del _df
