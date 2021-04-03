# Data Insights

`data_insights` helps the user transform Vendor Invoice data at scale leveraging pyspark. 
`main.py` is the driver file meant to be an entryway to execute the data processing functionality. 
`config.json` host's the sample configuration. Execute `config.json` against `main.py` by:
```
python main.py -f config.json
```

## Setup Python Environment

Requirements:
* Java Version `11` 
* Apache Spark Version `3.1.1` 
* Python Version `3.9.2`
  
## Package Installation

`data_insights` is fairly light weight. You can install the python package with `pip` by executing `pip install .` from the root of the repo, i.e. the level you find the file `setup.py`
