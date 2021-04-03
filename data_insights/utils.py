import logging
import sys

from pyspark.sql import SparkSession


def get_spark_session(name: str = "analytics") -> SparkSession:
    """Returns the spark session with the appropriate settings required for
    data processing."""
    return (
        SparkSession.builder.appName(name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


class Logger:
    """Generate the logger object to help with package logging requirements."""

    log = None

    def __init__(self, name="GLEAN"):
        _logger = logging.getLogger(name=name)
        _logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "[ %(asctime)s | %(name)s | %(levelname)s ] %(message)s",
            datefmt="%A %b %-d, %Y - %I:%M:%S %p",
        )
        ch.setFormatter(formatter)
        _logger.addHandler(ch)
        self.log = _logger


logger = Logger().log
