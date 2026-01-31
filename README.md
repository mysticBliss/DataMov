# DataMov

## Overview

This Python-based data processing tool uses Apache Spark to orchestrate data flows, monitor ETL processes, retrieve data from various sources, and perform save operations. It is designed for enterprise environments and compatible with Cloudera Data Platform (CDP) and modern Spark distributions. This utility allows users to extract data from sources such as Hive and Parquet, apply inline SQL transformations, validate data quality using Great Expectations, and subsequently store the processed data in Hive, Kudu, and Parquet.

## Features

- **Data Movement**: Extract, Transform, Load (ETL) between Hive, Kudu, Impala, and Filesystem (Parquet).
- **Transformation**: Inline SQL transformations.
- **Data Quality**: Integrated Great Expectations for data validation.
- **Monitoring**: automated tracking of ETL processes in a Hive table.
- **Configuration**: JSON-based configuration for easy management.
- **Enterprise Grade**: Python 3 support, Type Hinting, Logging, and Testing.

### Installation

1. Clone the repository.
2. Install dependencies:

```console
pip install -r requirements.txt
pip install -e .
```

### Build Package

To build the distributable wheel package:

```console
python setup.py bdist_wheel
```

### Requirements
- Python >= 3.8
- PySpark >= 3.0.0
- Great Expectations

## Usage

### Prepare your data flow configurations

Define your data movements in `data_movements_{env}.json`.

Example with **Great Expectations**:

```json
{
    "data_movements": [
        {
            "name": "hive-to-hive-voice-data",
            "active": true,
            "source_type": "hive",
            "source_sql": "select '1' as a, '2' as b",
            "destination_type": "hive",
            "destination_mode": "overwrite",
            "destination_table": "default.sample_etl",
            "destination_sql": "SELECT a, b FROM",
            "expectations": [
                {
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "a"}
                },
                {
                    "type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "a"}
                }
            ]
        }
    ]
}
```

### Run the data flow

**Standard Execution:**

```console
python main.py -f flow_name -c spark_config.json
```

**PySpark Submission:**

```console
pyspark --master yarn --conf spark.pyspark.virtualenv.enabled=true --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.bin.path=/path/to/datamov --conf spark.pyspark.python=/usr/bin/python3
```

## Testing

Run unit and integration tests:

```console
pytest tests/
```

## Structure

```console
├── datamov
│   ├── connectors
│   │   └── spark_manager
│   └── core
│       ├── config_reader
│       ├── data_flow
│       ├── data_movements
│       ├── data_processor
│       ├── engine
│       ├── logger
│       └── validator
├── scripts
├── tests
```
