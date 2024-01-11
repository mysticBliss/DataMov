# DataMov 

## Overview

This Python-based data processing tool uses Apache Spark to orchestrate data flows, monitor ETL processes, retrieve data from various sources, and perform save operations. It is compatible with Cloudera Data Platform (CDP), specifically CDP 7.1.8. This utility allows users to extract data from sources such as Hive and Parquet, apply inline SQL transformations to the extracted data, and subsequently store the processed data in Hive, Kudu, and Parquet. Its design is user-friendly, allowing non-Spark users to configure data movements via a JSON file and execute the main file by simply specifying the flow name. The code is written in standard Python without introducing any new dependencies. Additionally, this tool offers a seamless installation process. It has been tested in Oozie workflows and through the Command Line Interface (CLI).


### Installation

```console
pip install DataMov-1.0.0-py2.whl
```

### Requirements
Python 2.7

```console
├───datamov
│   ├───connectors
│   │   └───spark_manager
│   └───core
│       ├───config_reader
│       ├───data_flow
│       ├───data_movements
│       ├───engine
│       └───logger
├───scripts
```

## Usage

To use this tool, follow these steps:

### Prepare your data flow configurations and environment settings.

Every data movement necessitates the establishment of the datamovement. These definitions are included in the `data_movements_{env}.json` file. This file specifies the source and destination details for each data movement.

Similarly, each environment setup requires the definition of the environment. These definitions are included in the `environment_*.json` file. This file specifies the details for each environment setup.

### Define your data flows using the provided classes.

Let's proceed to design a data pipeline. We will introduce a new data movement value in the JSON configuration file named `data_movements_sample.json`.

```json
{
    "data_movements": [
        {
            "name": "hive-to-hive-voice-data",
            "description": "DataFlow that loads sample hive table to hive with select SQL inline transformation",
            "active": true,
            "source_execution_date": null,
            "source_frequency_value": null,
            "source_frequency_unit": null,
            "source_type": "hive",
            "source_format": null,
            "source_table": null,
            "source_sql": "select '1' as a, '2' as b,  '3' as c",
            "source_partition_column": null,
            "source_fs_path": null,
            "source_data_format": null,
            "destination_type": "hive",
            "destination_mode": "overwrite",
            "destination_table": "default.sample_etl",
            "destination_partitions": [],
            "destination_fs_path": null,
            "destination_fs_func": null,
            "destination_path": null,
            "destination_sql": "SELECT a, b FROM"
        }
    ]
}
```

In the given example, we're performing a selection operation on a source table that consists of three columns ( check field `source_sql`). After this selection, we're loading only two columns ( check field  `destination_sql`) into the final table.



### Instantiate the Engine class and load your data flow configurations.
### Run the data flow using the run_flow method.

Example
```console
python main.py -f flow_name -c spark_config.json
```

Create a new environment

```console
conda create -y -n py27 python=2.7
```

Or export your existing into a yml file and then import it.

```console
conda env export py27 > environment.yml

conda env create -f environment.yml

conda activate py27
```

Build  .whl Package

```console
python setup.py bdist_wheel
```


```console
pyspark --master yarn-client --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.bin.path=/home/saqib.tamli/datamov --conf spark.pyspark.python=/usr/bin/python2
```
## DataFlow

The DataFlow class represents a data movement definition. It initializes and manages various attributes related to data flow, such as source and destination paths, data formats, and execution frequencies. It includes methods to generate paths based on the data format and source execution date.

### Initialize a DataFlow instance
```console
data_flow = DataFlow(
    name="ExampleFlow",
    source_type="hive",
    source_fs_path="/path/to/source",
    source_execution_date="2023-12-18",
    source_data_format="parquet",
    destination_table="destination_table",
    destination_format="parquet"
)
```

### Generate paths based on the data format and execution date
```console
data_flow.generate_paths
```

## Engine

The Engine class orchestrates the execution of data flows. It loads a data flow and its associated configurations, initializes a Spark session, processes data using the DataProcessor, and manages the overall execution of the data flow.

### Load a data flow into the Engine
```console
executor = Engine(spark_config)
executor.load_data_flow(data_flow, environment_config)
executor.run_flow()
```

## Data Processor

The DataProcessor class handles data fetching, transformation, and saving operations. It includes methods to fetch data from different source types (Hive, Kudu, etc.), create temporary tables, process resultant data frames, and save data to destinations based on specified formats.

### Initialize DataProcessor with a Spark session

```console
data_processor = DataProcessor(spark)
```

### Fetch data from a source type
```console
data_processor.fetch_data("hive", "/path/to/hive_table", query=None)
```

### Save data to a destination
```console
data_processor.save_data(
    df,
    "/path/to/destination",
    format_type="parquet",
    mode="overwrite",
    partition_by="partition_column"
)
```
