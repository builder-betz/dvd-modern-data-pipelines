# Batch Data Orchestration

This project uses [Dagster](https://dagster.io/) for orchestrating the batch data pipeline, coordinating Airbyte data ingestion and dbt transformations.

## Overview

Dagster orchestrates the batch pipeline by:
1. Triggering Airbyte connections on a daily schedule
2. Triggering dbt transformations after successful data ingestion
3. Managing dependencies between data assets
4. Providing lineage visualization and monitoring

<div align="center">

![Dagster Lineage](./dagster/images/batch-lineage.png)

</div>

## Getting Started

### Local Development

First, install your Dagster code location as a Python package. 

```bash
cd 03-data-orchestration/batch/dagster
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Deployment

The project is deployed on Dagster+ (Dagster Cloud) for production orchestration.

<div align="center">

![Dagster Deployment Code Location](./dagster/images/batch-deployment-code-location.png)

</div>

## Runs

<div align="center">

![Dagster UI Running](./dagster/images/batch-run.png)

</div>

---
🔗 **Page Navigation**:  [Main](../../README.md) | [Batch](../../batch/00-data-pipelines/batch/README.md) | [Streaming](../../streaming/00-data-pipelines/batch/README.md) | [Prev](../../02-data-transformation/batch/README.md) | [Next](../../04-data-consumption/batch/README.md)