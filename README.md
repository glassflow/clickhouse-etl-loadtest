# GlassFlow Clickhouse ETL LoadTesting

A load testing tool for [GlassFlow Clickhouse ETL](https://github.com/glassflow/clickhouse-etl), designed to evaluate the performance and reliability of the glassflow clickhouse etl pipeine.

## Overview

This tool performs load testing on GlassFlow Clickhouse ETL by:
- Generating synthetic data using [glassgen](https://github.com/glassflow/glassgen)
- Sending data to Kafka topics
- Measuring the performance of data processing through the ETL pipeline
- Collecting and analyzing metrics

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Local machine with sufficient resources to run the test

## Installation

1. Clone this repository:
```bash
git clone https://github.com/glassflow/clickhouse-etl-loadtest
cd clickhouse-etl-loadtest
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

### Load testing parameters

The load test parameters are defined in `config/load_test_params.json`. Here are the available parameters:

### Test Parameters

| Parameter | Description | Example Range/Values |
|-----------|-------------|--------------|
| num_processes | Number of parallel processes | 4-12 (step: 2) |
| records_per_second | Records per second to generate | 10,000-50,000 (step: 10,000) |
| total_records | Total number of records to generate | 500,000-5,000,000 (step: 500,000) |
| duplication_rate | Rate of duplicate records | 0.1 (10% duplicates) |
| deduplication_window | Time window for deduplication | ["1h", "4h"] |
| max_batch_size | Max batch size for the sink | [5000] |
| max_delay_time | Max delay time for the sink | ["10s"] |


You can customize the test parameters by editing `load_test_params.json` or creating another config file. For each parameter, you can set:
- `min`: Minimum value
- `max`: Maximum value
- `step`: Increment between values
- `values`: Fixed list of values (if step is not used)

Example configuration:
```json
{
    "num_processes": {
        "min": 4,
        "max": 12,
        "step": 2
    },
    "records_per_second": {
        "min": 10000,
        "max": 50000,
        "step": 10000
    }
}
```

To limit the number of test variants, you can set `max_combinations` in the configuration file. This is useful when you want to test a subset of all possible combinations.

### Pipeline parameters

The pipeline configuration is defined in `config/glassflow/deduplication_pipeline.json`. This configuration file is used to set up the GlassFlow Clickhouse ETL pipeline and specify the connection details for Kafka and ClickHouse. The existing file in the repo connects to a locally running Kafka and ClickHouse, but you can update that file if your Kafka and ClickHouse are running remotely on a cloud.

#### Configuration Options

You can configure the pipeline in two ways:

1. **Local Setup (Default)**
   - Uses local Kafka and ClickHouse instances running in Docker
   - Start the services using:
   ```bash
   docker-compose up -d
   ```

2. **Remote Setup**
   - Connect to remote Kafka and ClickHouse instances
   - Add credentials and connection details in the pipeline config
   - Start only the GlassFlow services using:
   ```bash
   docker-compose -f docker-compose-glassflow.yaml up -d
   ```

For detailed information about the pipeline configuration structure and available options, please refer to the [GlassFlow ETL documentation](https://docs.glassflow.dev/pipeline/pipeline-configuration).

## Running the Tests

1. Configure your test parameters:
   - Edit `load_test_params.json` to set your desired parameter ranges
   - Optionally set `max_combinations` to limit the number of test variants
   - Save the configuration file

2. Start the required services:
```bash
docker-compose up -d
```

3. Run the load test:
```bash   
python main.py --test-id <your_test_id> --config load_test_params.json --pipeline-config deduplication_pipeline.json 
```

Additional options:
- `--no-resume`: Do not resume from previous test run
- `--results-dir`: Directory to store test results (default: 'results')
- `--glassflow-host`: Endpoint to reach glassflow (default: 'http://localhost:8080')


## Test Results

The test results are stored in the `results` directory with the following format:
- CSV file: `<test-id>_results.csv`

For example, if you ran a test with ID "test-001", the results would be in `results/test-001_results.csv`

### Metrics Collected

The following metrics are collected and analyzed for each test run:

| Metric | Description | Unit |
|--------|-------------|------|
| duration_sec | Total time taken for the test | seconds |
| result_num_records | Number of records processed | count |
| result_time_taken_publish_ms | Time taken to publish records to Kafka | milliseconds |
| result_time_taken_ms | Time taken to process records through the pipeline | milliseconds |
| result_rps_achieved | Records per second achieved during the test | records/second |
| result_avg_latency_ms | Average latency per record | milliseconds |
| result_success | Whether the test completed successfully | boolean |
| result_lag_ms | Lag between data generation and processing | milliseconds |

These metrics provide insights into:
- Overall test performance (duration, success rate)
- Data processing throughput (RPS)
- Processing efficiency (latency, lag)
- System reliability (success rate)

## Analyzing Results

The tool includes a results analysis script (`results.py`) that helps you analyze and visualize the test results. To analyze your test results, run:
```bash
python results.py --results-file results/<test-id>_results.csv
```

For example:
```bash
python results.py --results-file results/19_05_001_results.csv
```

The script will display:
- A list of all available columns in the results file
- A formatted table showing the important metrics for each test variant:
  - Variant ID
  - Time taken for the test
  - Number of records processed
  - Time taken to publish
  - RPS achieved
  - Time taken to process records
  - Average latency
  - Success status

The results are presented in a clear, tabular format with color-coded headers and values for better readability.

Error handling:
- If the results file is not found, an error message will be displayed in red
- Any other errors during analysis will be clearly reported

## Architecture

The load test by default runs on your local machine and interacts with:
- Kafka: Running in Docker for message streaming
- ClickHouse: Running in Docker for data storage
- GlassFlow ETL: Running in Docker for data processing

However the load test can also interact with Kafka and Clickhouse running in the cloud.

## Cleanup

Each iteration in the load test creates the needed kafka topics and clickhouse tables. It deletes those after the test is run. 