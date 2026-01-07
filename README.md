# Salesforce Marketing Cloud to Snowflake using dlt

Load Salesforce Marketing Cloud (SFMC) data into Snowflake using [dlt (data load tool)](https://dlthub.com/). This pipeline can be run locally, in a container, or inside Snowflake using **Snowflake Container Services (SPCS)**.

## Overview

This project demonstrates how to extract data from Salesforce Marketing Cloud using both REST and SOAP APIs, and load it into Snowflake using dlt. The implementation supports:

- **REST API**: Assets, Campaigns, and Journeys
- **SOAP API**: Email events (Sent, Open, Click, Bounce, Unsubscribe), Sends, Subscribers
- **Incremental loading**: Date-filtered loads for high-volume event data
- **Full loads**: Complete historical data for reference tables
- **Container deployment**: Dockerized for local or SPCS execution

## Blog Post

For a detailed walkthrough of running dlt inside Snowflake Container Services, see:

📖 **[Can You Run dlt Inside Snowflake? Part 2/2 - SPCS](https://www.sfrt.io/can-you-run-dlt-inside-snowflake-part-2-2-spcs/)**

## Features

- **Dual API Support**: Combines REST and SOAP APIs to access different SFMC data
- **OAuth2 Authentication**: Automatic token management with refresh for SOAP API
- **Smart Loading Strategies**:
  - Incremental loads for event data (last N days)
  - Full loads for reference data (Subscribers)
  - Merge mode for upserts
- **Flexible Configuration**: Easily adjust which objects to load and date ranges
- **SPCS Ready**: Dockerfile included for Snowflake Container Services deployment

## Data Sources

### REST API Endpoints

- **Assets** (`asset/v1/content/assets`): Content Builder assets
- **Campaigns** (`hub/v1/campaigns`): Campaign information
- **Journeys** (`interaction/v1/interactions`): Journey Builder interactions

### SOAP API Objects

- **SentEvent**: Email send events
- **OpenEvent**: Email open events
- **ClickEvent**: Email click events
- **BounceEvent**: Email bounce events
- **UnsubEvent**: Unsubscribe events
- **Send**: Send job information
- **Subscriber**: Subscriber profile data

## Prerequisites

### Salesforce Marketing Cloud

1. Create an **Installed Package** in SFMC Setup
2. Add a **Server-to-Server API Integration** component
3. Grant required permissions:
   - **REST API**: Read permissions for Assets, Campaigns, Journeys
   - **SOAP API**: Read permissions for Email, Subscribers, Tracking Events
4. Note down:
   - Client ID
   - Client Secret
   - Subdomain (e.g., `mc123456789`)

### Snowflake

1. Create a database and schema for the pipeline
2. Set up authentication:
   - User/password, or
   - Key-pair authentication, or
   - OAuth (if running in SPCS)

## Installation

### Local Setup

1. Clone the repository:

```bash
git clone https://github.com/martinseifertprojuventute/dlt_salesforce_mc_to_snowflake.git
cd dlt_salesforce_mc_to_snowflake
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure credentials in `.dlt/secrets.toml`:

```toml
[sources.salesforce_marketing_cloud_rest_source]
subdomain = "mc123456789"
client_id = "your_client_id"
client_secret = "your_client_secret"

[sources.salesforce_marketing_cloud_soap_source]
subdomain = "mc123456789"
client_id = "your_client_id"
client_secret = "your_client_secret"

[destination.snowflake.credentials]
database = "your_database"
username = "your_username"
password = "your_password"
warehouse = "your_warehouse"
role = "your_role"
```

4. Run the pipeline:

```bash
python load_salesforce_marketing_cloud.py
```

### Docker Deployment

1. Build the container:

```bash
docker build -t sfmc-to-snowflake .
```

2. Run the container:

```bash
docker run --env-file .env sfmc-to-snowflake
```

### Snowflake Container Services (SPCS)

For detailed SPCS deployment instructions, see the [blog post](https://www.sfrt.io/can-you-run-dlt-inside-snowflake-part-2-2-spcs/).

## Configuration

### Adjusting Data Sources

Edit `load_salesforce_marketing_cloud.py` to customize which SOAP objects to load:

```python
soap_objects = [
    {
        'object_type': 'SentEvent',
        'properties': ['SendID', 'SubscriberKey', 'EventDate', ...],
        'filter_property': 'EventDate',
        'days_back': 4,  # Adjust lookback window
        'full_load': False,  # Set True for complete history
        'primary_key': 'id'
    },
    # Add more objects...
]
```

### Date Range Configuration

Control how far back to load data:

```python
# In the pipeline run
load_info = pipeline.run(
    salesforce_marketing_cloud_soap_source(days_back=7)  # Last 7 days
)
```

## Project Structure

```
.
├── load_salesforce_marketing_cloud.py  # Main pipeline script
├── load-salesforce-marketing-cloud.yaml  # SPCS job specification
├── drop_pipeline.py                    # Utility to reset pipeline state
├── dockerfile                          # Container image definition
├── requirements.txt                    # Python dependencies
├── .dlt/
│   └── secrets.toml                   # Credentials (not in repo)
└── .gitignore
```

## Usage

### Running Locally

```bash
python load_salesforce_marketing_cloud.py
```

### Resetting Pipeline State

To perform a complete reload:

```bash
python drop_pipeline.py
```

Or uncomment in the main script:

```python
pipeline = dlt.pipeline(
    pipeline_name='salesforce_marketing_cloud_pipeline',
    destination='snowflake',
    dataset_name='salesforce_marketing_cloud',
    refresh="drop_sources"  # Drops tables and resets state
)
```

### Scheduling

- **Local/Docker**: Use cron or a task scheduler
- **SPCS**: Use Snowflake Tasks to trigger the container service
- **Orchestration**: Integrate with Airflow, Prefect, or Dagster

## Data Models

The pipeline creates the following tables in Snowflake:

- `assets`: Content Builder assets
- `campaigns`: Campaign metadata
- `journeys`: Journey Builder definitions
- `sentevents`: Email send events
- `openevents`: Email open events
- `clickevents`: Email click events (with URLs)
- `bounceevents`: Email bounce events (with reasons)
- `unsubevents`: Unsubscribe events
- `sends`: Send job information
- `subscribers`: Subscriber profiles
- `_dlt_loads`: dlt pipeline metadata
- `_dlt_version`: dlt schema version tracking

## Troubleshooting

### Token Expiration

- **REST API**: Tokens expire after ~20 minutes. For long-running extractions, consider implementing token refresh or splitting into smaller batches.
- **SOAP API**: Automatic token refresh is built-in using the `RefreshableOAuthPlugin`.

### SOAP API Errors

- Ensure the API Integration has all required SOAP API permissions
- Check that object properties are spelled correctly (case-sensitive)
- Some objects (e.g., Subscriber) don't support date filtering

### Rate Limits

Salesforce Marketing Cloud enforces API rate limits. If you encounter rate limiting:

- Reduce `days_back` parameter
- Add delays between API calls
- Contact Salesforce to increase your rate limits

## Dependencies

Key Python packages:

- `dlt[snowflake]>=0.4.0`: Data load tool with Snowflake support
- `zeep`: SOAP API client
- `lxml`: XML processing for SOAP
- `requests`: HTTP client for REST API

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this project for your own purposes.

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [Salesforce Marketing Cloud API Documentation](https://developer.salesforce.com/docs/marketing/marketing-cloud/overview)
- [Snowflake Container Services Documentation](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [Blog: Running dlt in Snowflake SPCS](https://www.sfrt.io/can-you-run-dlt-inside-snowflake-part-2-2-spcs/)

## Author

Martin Seifert

---

**Questions?** Open an issue or reach out on [LinkedIn](https://www.linkedin.com/in/martinseifert/).
