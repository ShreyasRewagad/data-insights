import argparse
import json
import os

parser = argparse.ArgumentParser(description="Generate Gleans from Vendor Invoices.")
parser.add_argument(
    "--config_file_path", "-f", type=str, help="path to the json configuration file."
)
args = parser.parse_args()
config_file_path = str(args.config_file_path).strip()
assert os.path.exists(config_file_path)
config: dict = json.load(fp=open(config_file_path, "r"))

from data_insights.data_loader import DataLoader
from data_insights.insights import (
    AccrualAlert,
    LargeMonthIncreaseMtd,
    NoInvoiceReceived,
    VendorNotSeenInAWhile,
)
from data_insights.pipeline import Pipeline
from data_insights.utils import get_spark_session, logger

logger.info(f"Input configuration: {json.dumps(config, sort_keys=True, indent=2)}")
_valid_gleans = {
    _.__name__: _
    for _ in [
        AccrualAlert,
        LargeMonthIncreaseMtd,
        NoInvoiceReceived,
        VendorNotSeenInAWhile,
    ]
}

# validate config
logger.info("Validating Input configuration.")
_ = {"Data", "Gleans", "OutputPath"}.difference(set(config.keys()))
assert len(_) == 0, f"The following specification(s) is/are missing: {', '.join(_)}"
assert isinstance(config["Data"], dict)
assert all(
    isinstance(_k, str) and isinstance(_v, dict) for _k, _v in config["Data"].items()
)
assert isinstance(config["Gleans"], dict)
assert all(
    isinstance(_k, str) and _k in _valid_gleans.keys() and isinstance(_v, dict)
    for _k, _v in config["Gleans"].items()
)

logger.info("Constructing Data Loaders")
_spark = get_spark_session()
_data_loaders = {
    _identifier: DataLoader(
        spark=_spark, **{_k: _v for _k, _v in _args.items() if _k != "spark"}
    )
    for _identifier, _args in config["Data"].items()
}

logger.info("Constructing Gleans")
_gleans = [
    _valid_gleans[_glean](**{_k: _data_loaders[_v] for _k, _v in _args.items()})
    for _glean, _args in config["Gleans"].items()
]

logger.info("Constructing Pipeline")
Pipeline(spark=_spark, gleans=_gleans, path=config["OutputPath"])
