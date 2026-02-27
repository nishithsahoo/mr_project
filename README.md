# Multi-Channel Marketing Data Processing Pipeline

A Python-based data processing pipeline for consolidating and standardizing multi-channel healthcare professional (HCP) engagement data from various marketing touchpoints.

## Overview

This pipeline processes data from multiple marketing channels (calls, digital details, events, and LMMR) into standardized formats for downstream analysis. It handles data normalization, filtering, and consolidation while maintaining comprehensive logging.

## Features

- **Multi-Pipeline Processing**: Processes 4 distinct data sources (Call, Edetail, Events, VAE/LMMR)
- **Data Consolidation**: Combines all processed outputs into a unified HCP dataset
- **Time-Based Filtering**: Retains configurable rolling window of recent months
- **Automated Cleanup**: Clears output directory before each run
- **Comprehensive Logging**: Logs all operations to `outputs/pipeline.log`
- **Error Handling**: Graceful error logging without full stack traces

## Directory Structure

```
mr_model/
├── main.py                 # Main execution script
├── requirements.txt        # Python dependencies
├── README.md              # This file
│
├── config/                # Configuration files
│   ├── call.json
│   ├── edetail.json
│   ├── events.json
│   ├── vae.json
│   └── hco_promotion.json
│
├── inputs/                # Input data files (CSV/Parquet)
│   ├── Call.csv
│   ├── Edetail.csv
│   ├── Events.csv
│   └── VAE.csv
│
├── outputs/               # Generated output files
│   ├── call_processed.csv
│   ├── edetail_processed.csv
│   ├── event_processed.csv
│   ├── vae_processed.csv
│   ├── hco_promotion.csv
│   └── pipeline.log
│
├── pipelines/             # Pipeline modules
│   ├── call.py
│   ├── edetail.py
│   ├── events.py
│   ├── vae.py
│   └── hco_promotion.py
│
└── utils/                 # Utility functions
    ├── dates.py          # Date/YRMO operations
    ├── local.py          # File system utilities
    ├── s3.py             # Data reading utilities
    └── logging.py        # Logger configuration
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Copy the project folder

2. Install required dependencies, if not installed:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Each pipeline has a JSON configuration file in the `config/` directory:

### Common Configuration Parameters

- **source.path**: Path to input data file
- **filters**: Pipeline-specific filtering criteria
  - **months_to_retain**: Number of recent months to keep (default: 7)
  - Product/channel specific filters
- **output.csv**: Path for output CSV file

### Example: `config/call.json`

```json
{
  "source": {
    "path": "inputs/Call.csv"
  },
  "filters": {
    "product_external_id_vod__c": "JP_708127-420004",
    "months_to_retain": 7
  },
  "output": {
    "csv": "outputs/call_processed.csv"
  }
}
```

## Usage

### Run All Pipelines

Execute all pipelines in sequence:

```bash
python main.py
```

This will:
1. Clean the `outputs/` directory (except `pipeline.log`)
2. Run pipelines in order: Call → Edetail → Events → VAE → HCO
3. Generate processed CSV files in `outputs/`
4. Log all operations to `outputs/pipeline.log`

### Run Individual Pipelines

To run a specific pipeline programmatically:

```python
from pipelines import call
from main import load_config

config = load_config("config/call.json")
result_df = call.run(config)
```

## Pipeline Descriptions

### 1. Call Pipeline (`pipelines/call.py`)
Processes face-to-face and virtual call data from sales representatives.

**Input Columns**: `child_account_identifier_vod__c`, `call_date_vod__c`, `call2_vod_id`, `recordtype_name`, `Action`, `product_external_id_vod__c`

**Output Columns**: `HCP_ID`, `ACTIVITY_DATE`, `YRMO`, `ID`, `CHANNEL`, `ACTION`

### 2. Edetail Pipeline (`pipelines/edetail.py`)
Processes digital engagement data from multiple email and edetail platforms (M3, CARENET, MEDPEER, NMO, JSTREAM).

**Channels Processed**:
- M3 Email variants (MR_KUN, QUIZ, MM, OPD)
- CARENET/MEDPEER edetails
- NMO edetails

**Engagement Metrics**: Delivered, Opened, Clicked

### 3. Events Pipeline (`pipelines/events.py`)
Processes conference and event attendance data.

**Input Columns**: `conference_id`, `customer_id`, `product_id`, `indication_id`, `channel`, `action`, `ACTVY_STRT_DT`

**Output Columns**: `HCP_ID`, `ACTIVITY_DATE`, `YRMO`, `ID`, `CHANNEL`, `ACTION`

### 4. VAE/LMMR Pipeline (`pipelines/vae.py`)
Processes Last Mile Marketing Reach (LMMR) data.

**Input Columns**: `customer_id`, `activity_date`, `sevc_id`, `action`, `product_id`

**Channel**: All records marked as "LMMR"

### 5. HCO Promotion Pipeline (`pipelines/hco_promotion.py`)
Consolidates outputs from all four pipelines into a single unified dataset.

**Input**: Processed CSV files from Call, Edetail, Events, and VAE pipelines

**Output**: Concatenated HCP-level dataset

## Data Processing Flow

```
Input Data (CSV/Parquet)
    ↓
Pipeline Processing:
    • Load data
    • Filter by product/criteria
    • Normalize columns
    • Convert dates
    • Add YRMO column
    • Apply time-based filtering (last N months)
    • Select output columns
    ↓
Output CSV Files
    ↓
HCO Consolidation (concatenate all outputs)
    ↓
Final Unified Dataset
```

## Output Format

All pipeline outputs share a common schema:

| Column | Type | Description |
|--------|------|-------------|
| HCP_ID | String | Healthcare professional identifier |
| ACTIVITY_DATE | Date | Date of activity (YYYY-MM-DD) |
| YRMO | String | Year-Month (YYYY-MM) |
| ID | String | Activity/event unique identifier |
| CHANNEL | String | Marketing channel name |
| ACTION | String | Action taken (e.g., Attended, Opened, Delivered) |

## Logging

All operations are logged to `outputs/pipeline.log`:

```
2026-02-26 19:35:17,632 INFO __main__ - Starting pipeline execution
2026-02-26 19:35:17,633 INFO __main__ - Running call pipeline with config config/call.json
2026-02-26 19:35:17,651 INFO pipelines.call - Saved Call output to outputs/call_processed.csv
2026-02-26 19:35:17,651 INFO __main__ - Completed call pipeline
```

The log file is overwritten on each run.

## Error Handling

Errors are logged with descriptive messages without full stack traces:

```
ERROR __main__ - Failed to run call pipeline: FileNotFoundError: [Errno 2] No such file or directory: 'inputs/Call.csv'
```

Execution stops on the first pipeline error.

## Requirements

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical operations
- **pyarrow**: Parquet file support
- **python-dateutil**: Date arithmetic for retention filtering

## Customization

### Adjusting Time Retention

Modify `months_to_retain` in any pipeline config:

```json
"filters": {
  "months_to_retain": 12  // Keep 12 months instead of default 7
}
```

### Adding New Filters

Add custom filters in pipeline configuration files:

```json
"filters": {
  "product_external_id_vod__c": "JP_708127-420004",
  "region": "Asia-Pacific"
}
```

Then modify the corresponding pipeline's `run()` function to apply the filter.

## Troubleshooting

### Common Issues

**Issue**: `FileNotFoundError` when running pipelines
- **Solution**: Ensure input CSV files exist in `inputs/` directory

**Issue**: `PermissionError` on pipeline.log
- **Solution**: Close any programs with the log file open

**Issue**: Empty output files
- **Solution**: Check filter criteria in config files (e.g., product_id mismatch)

**Issue**: Out of memory errors
- **Solution**: Process fewer months or filter data more aggressively
