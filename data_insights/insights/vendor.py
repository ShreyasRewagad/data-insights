from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import reduce
from typing import List, Tuple

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as sf
from pyspark.sql.types import (
    AtomicType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    TimestampType,
)

from data_insights.data_loader import DataLoader
from data_insights.insights.base import Insight, InsightLocation


@dataclass(frozen=True, eq=True)
class LargeMonthIncreaseMtd(Insight):
    """
    Insights based on Vendor expenditure.
    :param invoice_data_loader: A DataLoader object for vendor-invoice dataset.
    """

    # Initialization Arguments
    invoice_data_loader: DataLoader = field(init=True, default=None)

    # Inferred Arguments
    vendor_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("canonical_vendor_id", StringType())
    )
    invoice_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_id", StringType())
    )
    invoice_date_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_date", TimestampType())
    )
    amount_spent_column: Tuple[str, AtomicType] = field(
        init=False, default=("total_amount", FloatType())
    )
    insight_location: InsightLocation = field(
        init=False, default=InsightLocation.Vendor
    )
    window: int = field(init=False, default=365)
    expected_columns: List[str] = field(init=False, default=None)

    def __post_init__(self):
        self._check_column_types(
            data_loader=self.invoice_data_loader,
            column_types=[
                self.vendor_id_column,
                self.invoice_id_column,
                self.invoice_date_column,
                self.amount_spent_column,
            ],
        )
        object.__setattr__(
            self,
            "expected_columns",
            [
                self.vendor_id_column[0],
                "insight_date",
                "insight_text",
            ],
        )
        super(LargeMonthIncreaseMtd, self).__post_init__()

    def __call__(self) -> DataFrame:
        _df = (
            self.invoice_data_loader.df.groupby(
                self.vendor_id_column[0],
                self.invoice_id_column[0],
                self.invoice_date_column[0],
            )
            .agg(sf.max(self.amount_spent_column[0]).alias(self.amount_spent_column[0]))
            .withColumn(
                "invoice_month",
                sf.trunc(sf.col(self.invoice_date_column[0]), "month"),
            )
        )
        _df = _df.groupby(
            self.vendor_id_column[0],
            "invoice_month",
        ).agg(sf.sum(self.amount_spent_column[0]).alias("monthly_expense"))
        _window = (
            Window.partitionBy(self.vendor_id_column[0])
            .orderBy(sf.unix_timestamp("invoice_month"))
            .rangeBetween(self.window * -1 * 24 * 60 * 60, 0)
        )
        _df = _df.select(
            "*",
            sf.mean("monthly_expense")
            .over(_window)
            .alias(f"average_amount_spent_past_{self.window}_days"),
        )
        _df = _df.withColumn(
            "percentage_change",
            sf.when(
                (sf.col("monthly_expense").isNotNull())
                & (sf.col("monthly_expense") > 0)
                & (sf.col(f"average_amount_spent_past_{self.window}_days").isNotNull())
                & (sf.col(f"average_amount_spent_past_{self.window}_days") > 0),
                (
                    100
                    * (
                        sf.col("monthly_expense")
                        - sf.col(f"average_amount_spent_past_{self.window}_days")
                    )
                )
                / sf.col(f"average_amount_spent_past_{self.window}_days"),
            ).otherwise(sf.lit(0)),
        )
        _df = reduce(
            lambda _left, _right: _left.unionByName(_right),
            [
                _
                for _ in [
                    # trigger if monthly spend > $10K, > 50%
                    _df.filter(
                        (sf.col("monthly_expense") > 10000)
                        & (sf.col("percentage_change") > 50)
                    ),
                    # If monthly spend is less than $10K, > 200%.
                    _df.filter(
                        (sf.col("monthly_expense") > 1000)
                        & (sf.col("monthly_expense") < 10000)
                        & (sf.col("percentage_change") > 200)
                    ),
                    # If less than $1K, > 500%.
                    _df.filter(
                        (sf.col("monthly_expense") > 100)
                        & (sf.col("monthly_expense") < 1000)
                        & (sf.col("percentage_change") > 500)
                    ),
                ]
                if _.count() > 0
            ],
        )
        return _df.select(
            self.vendor_id_column[0],
            sf.col("invoice_month").alias("insight_date"),
            "monthly_expense",
            sf.col(f"average_amount_spent_past_{self.window}_days"),
            sf.concat(
                sf.lit("Monthly spend with `"),
                sf.col(self.vendor_id_column[0]),
                sf.lit("` is $"),
                sf.round(sf.col("monthly_expense"), 2),
                sf.lit(" ("),
                sf.round(sf.col(f"average_amount_spent_past_{self.window}_days"), 2),
                sf.lit("%) higher than average."),
            ).alias("insight_text"),
        )


@dataclass(frozen=True, eq=True)
class NoInvoiceReceived(Insight):
    """
    Insights based on Invoice Receipt Cadence
    :param invoice_data_loader: A DataLoader object for vendor-invoice dataset.
    """

    # Initialization Arguments
    invoice_data_loader: DataLoader = field(init=True, default=None)

    # Inferred Arguments
    vendor_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("canonical_vendor_id", StringType())
    )
    invoice_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_id", StringType())
    )
    invoice_date_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_date", TimestampType())
    )

    insight_location: InsightLocation = field(
        init=False, default=InsightLocation.Vendor
    )
    expected_columns: List[str] = field(init=False, default=None)

    def __post_init__(self):
        self._check_column_types(
            data_loader=self.invoice_data_loader,
            column_types=[
                self.vendor_id_column,
                self.invoice_id_column,
                self.invoice_date_column,
            ],
        )
        object.__setattr__(
            self,
            "expected_columns",
            [
                self.vendor_id_column[0],
                "insight_date",
                "insight_text",
            ],
        )
        super(NoInvoiceReceived, self).__post_init__()

    @staticmethod
    def _date_to_utc(value: str) -> datetime:
        return pd.to_datetime(value).replace(tzinfo=timezone.utc)

    def _get_timeline_df(self, start: date, end: date, cadence: str) -> DataFrame:
        return self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "date": [
                        self._date_to_utc(_).strftime("%Y-%m-%d")
                        for _ in pd.date_range(start=start, end=end, freq=cadence)
                    ],
                }
            )
        ).select(
            sf.to_date(sf.to_utc_timestamp(sf.col("date"), tz="UTC")).alias("date")
        )

    @staticmethod
    def _explode_trigger_records(
        df: DataFrame,
        group_columns: List[str],
        columns_to_keep: List[str],
        start_date_column: str,
        end_date_column: str,
    ) -> DataFrame:
        def _pd_trigger_fx(pdf: pd.DataFrame) -> pd.DataFrame:
            _value_dict = (
                pdf[[*group_columns, start_date_column, end_date_column]]
                .iloc[0]
                .to_dict()
            )
            return pd.DataFrame(
                {
                    **{_col: _value_dict[_col] for _col in columns_to_keep},
                    "date": pd.date_range(
                        start=_value_dict[start_date_column],
                        end=_value_dict[end_date_column]
                        - timedelta(seconds=(24 * 60 * 60) + 1),
                        freq="D",
                    ),
                    "day": _value_dict[start_date_column].day,
                }
            )

        _schema = df.select(*columns_to_keep).schema
        _schema.add(StructField(name="date", dataType=TimestampType(), nullable=True))
        _schema.add(StructField(name="day", dataType=IntegerType(), nullable=True))
        return (
            df.groupby(*group_columns)
            .applyInPandas(func=_pd_trigger_fx, schema=_schema)
            .withColumn("date", sf.to_date(sf.col("date")))
        )

    def _get_triggers(
        self,
        row_range: Tuple[int, int],
        date_trunc: str,
        cadence: str,
        lags: List[int],
    ):
        _df = self.invoice_data_loader.df.select(
            self.vendor_id_column[0],
            self.invoice_id_column[0],
            sf.to_date(sf.col(self.invoice_date_column[0])).alias(
                self.invoice_date_column[0]
            ),
            sf.trunc(sf.col(self.invoice_date_column[0]), date_trunc).alias(
                "invoice_cadence"
            ),
        )

        # construct the monthly timeline
        _date_range = (
            _df.agg(
                sf.min(sf.col("invoice_cadence")).alias("start"),
                sf.max(sf.col("invoice_cadence")).alias("end"),
            )
            .select(
                sf.col("start").cast(StringType()),
                sf.col("end").cast(StringType()),
            )
            .collect()[0]
        )
        _start = self._date_to_utc(_date_range["start"])
        _end = self._date_to_utc(_date_range["end"])
        _df = (
            _df.select(self.vendor_id_column[0])
            .dropDuplicates()
            .crossJoin(
                sf.broadcast(
                    self._get_timeline_df(
                        start=_start, end=_end, cadence=cadence
                    ).withColumnRenamed("date", "invoice_cadence")
                )
            )
            .join(
                other=_df, on=[self.vendor_id_column[0], "invoice_cadence"], how="left"
            )
        )
        del _start, _end, _date_range

        # at a monthly cadence get the first receipt of invoice
        _df = _df.groupby(self.vendor_id_column[0], "invoice_cadence").agg(
            sf.min(self.invoice_date_column[0]).alias("first_invoice_date"),
            sf.countDistinct(self.invoice_id_column[0]).alias("n_invoices"),
        )

        # Sliding Window to capture Vendor invoice charge behavior
        _window = (
            Window.partitionBy(sf.col(self.vendor_id_column[0]))
            .orderBy(sf.unix_timestamp(sf.col("invoice_cadence")))
            .rowsBetween(*row_range)
        )
        _df = _df.select(
            "*",
            sf.min("first_invoice_date")
            .over(window=_window)
            .alias("expected_invoice_date"),
            sf.collect_set("first_invoice_date")
            .over(window=_window)
            .alias("expected_invoice_dates"),
        )
        del _window

        # Generate the expected first date for vendor invoice receipt
        _df = _df.withColumn(
            "corrected_expected_invoice_date",
            sf.to_date(
                sf.concat_ws(
                    "-",
                    sf.year(sf.col("invoice_cadence")),
                    sf.month(sf.col("invoice_cadence")),
                    sf.dayofmonth(sf.col("expected_invoice_date")),
                )
            ),
        )

        # Update the first invoice receipt date to start of the next month as
        # vendor fails to provide a invoice within the month.
        _df = _df.withColumn(
            "corrected_first_invoice_date",
            sf.when(
                sf.col("first_invoice_date").isNull(),
                sf.to_date(
                    sf.trunc(
                        sf.date_add(sf.col("invoice_cadence"), days=32),
                        "month",
                    )
                ),
            ).otherwise(sf.col("first_invoice_date")),
        )

        _window_lag = Window.partitionBy(self.vendor_id_column[0]).orderBy(
            "invoice_cadence"
        )
        _df = _df.select(
            "*",
            *[
                sf.lag(col=sf.col("n_invoices"), offset=_lag)
                .over(_window_lag)
                .alias(f"n_invoices_lag_{_lag}")
                for _lag in lags
            ],
        )
        del _window_lag

        # filter out the trigger records where vendors failed to meet the invoice expectation date
        _df = _df.filter(
            (
                (sf.col("n_invoices").isNull())
                | (
                    sf.col("corrected_first_invoice_date")
                    > sf.col("corrected_expected_invoice_date")
                )
            )
            & reduce(
                lambda _l, _r: (_l & _r),
                [
                    *[sf.col(f"n_invoices_lag_{_lag}").isNotNull() for _lag in lags],
                    *[(sf.col(f"n_invoices_lag_{_lag}") > 0) for _lag in lags],
                ],
            )
        )
        return _df.select(
            self.vendor_id_column[0],
            "invoice_cadence",
            sf.col("corrected_expected_invoice_date").alias("start"),
            sf.col("corrected_first_invoice_date").alias("end"),
        )

    def __call__(self) -> DataFrame:
        # Generate the monthly triggers
        _monthly_case_df = self._get_triggers(
            row_range=(-3, -1),
            date_trunc="month",
            cadence="MS",
            lags=[1, 2, 3],
        ).withColumn("trigger", sf.lit("monthly"))

        # generate quarterly triggers and remove the vendor's present within
        # the monthly trigger
        _quarterly_case_df = (
            self._get_triggers(
                row_range=(
                    -2,
                    -1,
                ),
                date_trunc="quarter",
                cadence="QS",
                lags=[1, 2],
            )
            .join(
                other=(
                    _monthly_case_df.select(
                        self.vendor_id_column[0],
                        sf.trunc(sf.col("invoice_cadence"), "quarter").alias(
                            "invoice_cadence"
                        ),
                    ).dropDuplicates()
                ),
                on=[self.vendor_id_column[0], "invoice_cadence"],
                how="left_anti",
            )
            .withColumn("trigger", sf.lit("quarterly"))
        )
        _df = self._explode_trigger_records(
            df=_monthly_case_df.unionByName(_quarterly_case_df),
            group_columns=[
                self.vendor_id_column[0],
                "invoice_cadence",
                "start",
                "end",
            ],
            columns_to_keep=[self.vendor_id_column[0]],
            start_date_column="start",
            end_date_column="end",
        )
        del _monthly_case_df, _quarterly_case_df
        return _df.select(
            self.vendor_id_column[0],
            sf.col("date").alias("insight_date"),
            sf.concat(
                sf.lit("`"),
                sf.col(self.vendor_id_column[0]),
                sf.lit("` generally charges between on "),
                sf.col("day"),
                sf.lit(" day of each month invoices are sent. On "),
                sf.col("date"),
                sf.lit(", an invoice from `"),
                sf.col(self.vendor_id_column[0]),
                sf.lit("` has not been received."),
            ).alias("insight_text"),
        )
