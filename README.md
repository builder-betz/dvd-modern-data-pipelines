## Data Engineering Capstone Project: Batch and Streaming Pipelines

<div align="center">

![header](./images/header_text.png)

</div>

## Table of Contents<br/>
- [Business Context](#business-context)
- [Objective](#objective)
- [Consumers](#consumers)
- [Business Questions](#business-questions)
- [Dashboards](#dashboards)
- [Source Datasets](#source-datasets)
- [Solution Architecture](#solution-architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure and Navigation](#respository-structure-and-navigation)
- [Lessons learned](#lessons-learned)
- [Technical Debt and Improvement Opportunities](#technical-debt-and-improvement-opportunities)

## Business Context

The DVD rental business operates globally, with physical stores across multiple regions and an online platform where users browse catalogues, discover content, and add rentals to their baskets.

Customer interactions span both in-store transactions and online activity, creating a need for visibility into **long-term rental behaviour** as well as **real-time user engagement**. Together, these insights help the business better understand demand patterns, emerging trends, and opportunities for growth.

[🔝 Return to TOC](#table-of-contents)

## Objective

This project demonstrates an end-to-end batch and streaming data pipelines designed to answer key business questions for a DVD rental business.

It combines **batch analytics** (historical rentals, catalogue performance) with **real-time streaming analytics** (user clicks, activity trends), orchestrated and transformed using modern data-engineering platforms.

[🔝 Return to TOC](#table-of-contents)

## Consumers

The primary consumers of this data platform are data analysts and business operations teams, who rely on both historical insights and real-time events to support reporting, monitoring, and decision-making.

[🔝 Return to TOC](#table-of-contents)

## Business Questions

How can we analyse **user behaviour and content trends** using both historical (batch) and real-time (streaming) data to enable informed, timely, and actionable insights? 
<br/>
<br/>
*Detailed, pipeline-specific business questions are defined and addressed within the individual batch and streaming pipeline pages.*

[🔝 Return to TOC](#table-of-contents)

## Dashboards

The dashboards answer the business questions by visualising both historical and real-time user behaviour and content trends. All dashboards are hosted in Preset, a cloud-based analytics platform.  

- **Batch dashboard** presents historical trends, aggregates, and catalogue performance.

- **Streaming dashboard** presents near-real-time activity and emerging behavioural patterns, with automatic 10-second refreshes for timely visibility into live user interactions.

Detailed dashboards and chart breakdowns are available on the individual [batch](00-data-pipelines/batch/README.md) and [streaming](00-data-pipelines/streaming/README.md) pipeline pages.

<br/>
<br/>

<div align="center">

![Dashboards](./images/dashboards.gif)

</div>



<br/>

[🔝 Return to TOC](#table-of-contents)


## Source datasets

| Source name | Source type | Source documentation | Frequency | 
| - | - | - | - |
| DVD Rentals database | PostgreSQL database | https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/ | Daily |
| DVD Clicks (Synthetic) | Kafka Streaming Producer | [Synthetic Data](./00-data-source/streaming/README.md#clickstream-dataset) | Real-time | 

<br/>

[🔝 Return to TOC](#table-of-contents)

## Solution Architecture

This project uses a **hybrid batch–streaming architecture** to support both historical and real-time analytics.

- The [**batch pipeline**](./batch/00-data-pipelines/batch/README.md) is optimised for reliable historical analysis and dimensional modelling, using **Databricks** for scalable processing and a lakehouse serving layer.

- The [**streaming pipeline**](./streaming/00-data-pipelines/streaming/README.md) is optimised for low-latency, near-real-time insights, using **ClickHouse** to support sub-second analytical queries.

The pipelines intersect at the serving layer, where curated reference and dimension tables from the lakehouse are shared with the streaming pipeline to enrich real-time events.

<div align="center">

![Architecture Diagram](./images/pipelines.gif)

</div>

For detailed designs and implementation details, refer to the [Batch](./batch/00-data-pipelines/batch/README.md) and [Streaming](./streaming/00-data-pipelines/batch/README.md) pipeline pages.

[🔝 Return to TOC](#table-of-contents)

## Tech Stack

| Layer | Batch Pipeline | Streaming Pipeline |
|:---|:---|:---|
| **Data Source** | <img src="https://cdn.simpleicons.org/postgresql/4169E1" width="24" height="24"> AWS RDS (PostgreSQL) | <img src="https://cdn.simpleicons.org/apachekafka/231F20" width="24" height="24"> Kafka (Confluent Cloud) |
| **Ingestion** | <img src="https://cdn.simpleicons.org/airbyte/615EFF" width="24" height="24"> Airbyte | <img src="https://cdn.simpleicons.org/clickhouse/FFCC00" width="24" height="24"> ClickPipes (ClickHouse Cloud) |
| **Event Producer** | - | <img src="https://cdn.simpleicons.org/python/3776AB" width="24" height="24"> Python (confluent-kafka >=2.3.0) |
| **Data Warehouse** | <img src="https://cdn.simpleicons.org/databricks/FF3621" width="24" height="24"> Databricks (Unity Catalog) | <img src="https://cdn.simpleicons.org/clickhouse/FFCC00" width="24" height="24"> ClickHouse Cloud |
| **Transformation** | <img src="https://raw.githubusercontent.com/dbt-labs/dbt-core/main/core/dbt/include/global_project/dbt_logo.svg" width="24" height="24" alt="dbt"> dbt (dbt-core 1.10.4,<br/>dbt-databricks 1.10.4) | <img src="https://cdn.simpleicons.org/clickhouse/FFCC00" width="24" height="24"> ClickHouse Materialized Views |
| **Orchestration** | <img src="https://raw.githubusercontent.com/dagster-io/dagster/master/docs/next/public/images/dagster-logo.png" width="24" height="24" alt="Dagster"> Dagster+ (Dagster 1.12.7) | - |
| **Visualization** | <img src="https://cdn.simpleicons.org/apachesuperset/FF6B35" width="24" height="24"> Preset | <img src="https://cdn.simpleicons.org/apachesuperset/FF6B35" width="24" height="24"> Preset |

### Development Tools

<img src="https://cdn.simpleicons.org/python/3776AB" width="24" height="24"> **Python** 3.13+ | <img src="https://cdn.simpleicons.org/pypi/3775A9" width="24" height="24"> **pip** | <img src="https://cdn.simpleicons.org/anaconda/44A833" width="24" height="24"> **conda** | <img src="https://cdn.simpleicons.org/git/F05032" width="24" height="24"> **Git** | **VS Code**

[🔝 Return to TOC](#table-of-contents)

## Respository Structure and Navigation

>**📍 Start Here**
>If you’re new to this repository, begin with the pipeline overviews in `00-data-pipelines` (for example, [00-data-pipelines/batch](./00-data-pipelines/batch/README.md) and [00-data-pipelines/streaming](./00-data-pipelines/streaming/README.md)). These pages provide a high-level view of the batch and streaming pipelines and guide you through the rest of the project. The remaining directories contain pipeline-specific artefacts and further details for each stage of the data lifecycle.

```
.
├── 00-data-pipelines
├── 00-data-source
├── 01-data-ingestion
├── 02-data-transformation
├── 03-data-orchestration
├── 04-data-consumption
└── images
```

[🔝 Return to TOC](#table-of-contents)


## Lessons Learned

- **Design & Modelling**: Defining project structure and ERDs early greatly improves data model consistency and development speed.

- **Data Lineage**: Reviewing lineage graphs helps identify modelling inconsistencies and dependency issues early.  Adding metadata (such as Kafka client IDs) improves lineage visibility and operational traceability.

- **Ingestion & Infrastructure**: Airbyte performance depends heavily on instance sizing (used XXL), and operational housekeeping must be planned upfront.

- **Orchestration**: Scheduling granularity (for example, daily dimension refreshes) and environment differences can impact deployments.

- **Analytics & Dashboards**: Dataset schema choices (convert timestamp_ntz to timestamps for compatibility, and remove redundant date columns) as they affect compatibility and performance.

- **Orchestration**: dim_date does not have natural upstream dependencies, so it requires a dedicated daily cron schedule; otherwise, downstream models depending on it are not triggered.

- **Orchestration & Modelling**: While dbt Core allowed identical model names across Bronze and Silver layers locally, this led to asset conflicts in Dagster+, requiring clearer layer-specific naming.

[🔝 Return to TOC](#table-of-contents)

## Technical Debt & Improvement Opportunities

- **Data Freshness & Loads**: Align snapshot frequencies with refresh schedules and introduce CDC or incremental loading where appropriate.

- **Reliability & Quality**: Include further testing, logging, and monitoring—especially for streaming pipelines and ingestion tools.

- **Deployment & Automation**: Introduce CI/CD, Terraform and containerised execution (for example, ECS for Kafka producers).

- **Reference Data Management**: Improve reference data sharing between lakehouse and real-time analytics engines.

[🔝 Return to TOC](#table-of-contents)

---
🔗 **Page Navigation**:  Main | [Batch](./00-data-pipelines/batch/README.md) | [Streaming](./00-data-pipelines/streaming/README.md) | Prev | [Next](./00-data-pipelines/batch/README.md)

🔗 **Batch Pipeline Navigation**: 
[Data Source](./00-data-source/batch/README.md)
| [Data Ingestion](./01-data-ingestion/batch/README.md)
| [Data Transformation](./02-data-transformation/batch/README.md)
| [Data Orchestration](./03-data-orchestration/batch/README.md)
| [Data Consumption](./04-data-consumption/batch/README.md) 

🔗 **Streaming Pipeline Navigation**: 
[Data Source](./00-data-source/streaming/README.md)
| [Data Ingestion](./01-data-ingestion/streaming/README.md)
| [Data Transformation](./02-data-transformation/streaming/README.md)
| [Data Consumption](./04-data-consumption/streaming/README.md) 
