from dataclasses import dataclass, field
from typing import List, Tuple

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import AtomicType, StringType, TimestampType

from data_insights.data_loader import DataLoader
from data_insights.insights.base import Insight, InsightLocation


@dataclass(frozen=True, eq=True)
class VendorNotSeenInAWhile(Insight):
    """
    Insights based on Vendor-Invoice History.
    :param invoice_data_loader: A DataLoader object for vendor-invoice dataset.
    :param window: Number of days limit between consecutive vendor - invoices.
    """

    # Initialization Arguments
    invoice_data_loader: DataLoader = field(init=True, default=None)
    window: int = field(init=True, default=90)

    # Inferred Arguments
    vendor_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("canonical_vendor_id", StringType())
    )
    invoice_date_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_date", TimestampType())
    )
    invoice_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("invoice_id", StringType())
    )
    insight_location: InsightLocation = field(
        init=False, default=InsightLocation.Invoice
    )
    expected_columns: List[str] = field(init=False, default=None)

    def __post_init__(self):
        self._check_column_types(
            data_loader=self.invoice_data_loader,
            column_types=[
                self.vendor_id_column,
                self.invoice_date_column,
                self.invoice_id_column,
            ],
        )
        object.__setattr__(
            self,
            "expected_columns",
            [
                self.vendor_id_column[0],
                self.invoice_id_column[0],
                "insight_date",
                "insight_text",
            ],
        )
        super(VendorNotSeenInAWhile, self).__post_init__()

    def __call__(self) -> DataFrame:
        _df = self.invoice_data_loader.df.groupby(
            self.vendor_id_column[0],
            self.invoice_id_column[0],
        ).agg(
            sf.max(sf.col(self.invoice_date_column[0])).alias(
                self.invoice_date_column[0]
            )
        )

        # get the previous invoice date
        _window = Window.partitionBy(self.vendor_id_column[0]).orderBy(
            self.invoice_date_column[0]
        )
        _df = (
            _df.select(
                "*",
                sf.lag(col=sf.col(self.invoice_date_column[0]), offset=1)
                .over(_window)
                .alias("previous_invoice_date"),
            )
            .filter(sf.col("previous_invoice_date").isNotNull())
            .withColumn(
                "invoice_date_diff_days",
                sf.datediff(
                    sf.col(self.invoice_date_column[0]),
                    sf.col("previous_invoice_date"),
                ),
            )
            .withColumn(
                "months_since_latest_invoice",
                sf.months_between(
                    sf.col(self.invoice_date_column[0]),
                    sf.col("previous_invoice_date"),
                ),
            )
            .filter(sf.col("invoice_date_diff_days") > self.window)
        )
        return _df.select(
            "*",
            sf.to_date(sf.col(self.invoice_date_column[0])).alias("insight_date"),
            sf.concat(
                sf.lit("First new bill in "),
                sf.floor(sf.col("months_since_latest_invoice")),
                sf.lit(" months from vendor `"),
                sf.col(self.vendor_id_column[0]),
                sf.lit("`"),
            ).alias("insight_text"),
        )


@dataclass(frozen=True, eq=True)
class AccrualAlert(Insight):
    """
    Insights based on Invoice-line items coverage period.
    :param invoice_data_loader: A DataLoader object for vendor-invoice dataset.
    :param line_item_data_loader: A DataLoader object for invoice-line item dataset.
    :param window: Number of days limit between `invoice_date` and `period_end_date`
    """

    # Initialization Arguments
    invoice_data_loader: DataLoader = field(init=True, default=None)
    line_item_data_loader: DataLoader = field(init=True, default=None)
    window: int = field(init=True, default=90)

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
    line_item_id_column: Tuple[str, AtomicType] = field(
        init=False, default=("line_item_id", StringType())
    )
    period_end_date_column: Tuple[str, AtomicType] = field(
        init=False, default=("period_end_date", TimestampType())
    )
    insight_location: InsightLocation = field(
        init=False, default=InsightLocation.Invoice
    )
    expected_columns: List[str] = field(init=False, default=None)

    def __post_init__(self):
        self._check_column_types(
            data_loader=self.invoice_data_loader,
            column_types=[
                self.vendor_id_column,
                self.invoice_date_column,
                self.invoice_id_column,
                self.period_end_date_column,
            ],
        )
        self._check_column_types(
            data_loader=self.line_item_data_loader,
            column_types=[
                self.invoice_id_column,
                self.line_item_id_column,
                self.period_end_date_column,
            ],
        )
        object.__setattr__(
            self,
            "expected_columns",
            [
                self.vendor_id_column[0],
                self.invoice_id_column[0],
                "insight_date",
                "insight_text",
            ],
        )
        super(AccrualAlert, self).__post_init__()

    def __call__(self) -> DataFrame:
        _df = self.invoice_data_loader.df.select(
            self.invoice_id_column[0],
            self.vendor_id_column[0],
            self.invoice_date_column[0],
        ).join(
            self.line_item_data_loader.df.select(
                self.invoice_id_column[0],
                self.line_item_id_column[0],
                self.period_end_date_column[0],
            ),
            on=self.invoice_id_column[0],
            how="left",
        )

        _group_by_columns = [
            self.vendor_id_column[0],
            self.invoice_id_column[0],
        ]
        return (
            _df.groupby(*_group_by_columns, self.invoice_date_column[0])
            .agg(
                sf.max(sf.col(self.period_end_date_column[0])).alias(
                    self.period_end_date_column[0]
                )
            )
            .filter(
                sf.col(self.period_end_date_column[0])
                > sf.date_add(sf.col(self.invoice_date_column[0]), self.window)
            )
            .select(
                *_group_by_columns,
                sf.to_date(sf.col(self.invoice_date_column[0])).alias("insight_date"),
                sf.concat(
                    sf.lit("Line items from vendor `"),
                    sf.col(self.vendor_id_column[0]),
                    sf.lit("` in this invoice cover future periods ("),
                    sf.date_format(
                        sf.col(self.period_end_date_column[0]), "yyyy-MM-dd"
                    ),
                    sf.lit(")"),
                ).alias("insight_text"),
            )
        )
