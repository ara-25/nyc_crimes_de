# Crime Analytics (New York City) - Data Engineering Project
This repo contains the final project implemented for the [Data Engineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) course.

## Introduction

Crimes are a major contributor to social and economic unrest in metropolitan areas. This project aims to develop a workflow to ingest and process urban crime data, specifically for New York city, for downstream analysis.

## Dataset
The [NYPD Complaint Data Historic](https://data.cityofnewyork.us/Public-Safety/NYC-crime/qb7u-rbmr) dataset is used. It can be obtained conveniently through the [Socrata API](https://dev.socrata.com/foundry/data.cityofnewyork.us/qb7u-rbmr). This dataset is updated at a quarterly interval.

Each row denotes a crime occurrence. Details include information about the time, location and descriptive categorizations of the crime events. 

## Tools

The following components were utilized to implement the required solution:
* Data Ingestion: [Socrata API](https://dev.socrata.com/foundry/data.cityofnewyork.us/qb7u-rbmr) (used via the [sodapy](https://pypi.org/project/sodapy/) python client).
* Infrastructure as Code: Terraform
* Workflow Management: Airflow
* Data Lake: Google Cloud Storage
* Data Warehouse: Google BigQuery
* Data Transformation: Spark via Google Dataproc
* Reporting: Google Data Studio

### Architecture
![](res/2022-05-03-00-32-49.png)

## Steps to Reproduce

* Install the below tools:
  * [Terraform](https://www.terraform.io/downloads)
  * [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk#deb)





