from setuptools import find_packages, setup

setup(
    name="data_insights",
    version="0.0.1",
    python_requires=">3.9",
    author="Shreyas Rewagad",
    author_email="shreyas.rewagad@gmail.com",
    description="Uncover insights from data using PySpark",
    packages=find_packages(),
    install_requires=[
        "pandas==1.2.3",
        "pyarrow==3.0.0",
        "pyspark==3.1.1",
    ],
)
