# real_estate_data_pipeline

This project implements a **serverless ETL pipeline** using **AWS Lambda**, triggered automatically when a new CSV file is uploaded to an **S3 bucket**.  
The Lambda function extracts the file, transforms the data, loads it into **Snowflake**, and indexes it in **Elasticsearch** — all dynamically.

---


**Flow:**
1. A new CSV is uploaded to an AWS S3 bucket.
2. S3 automatically triggers a Lambda function.
3. Lambda:
   - Downloads and cleans/transforms the CSV.
   - Loads transformed data into a Snowflake table.
   - Indexes the same data in an Elasticsearch index.


---

## Setup Instructions

1. Create & Configure AWS S3

- Create a bucket named (for example): `real-estate-transactions-data`
- Upload a sample CSV file to the S3 bucket to test the trigger.

---

2. Create a Table in Snowflake

Run the following SQL script in your Snowflake worksheet (replace database, schema, and table names as needed):

```sql
CREATE DATABASE <DB_NAME>;
USE DATABASE <DB_NAME>;
CREATE SCHEMA <SCHEMA_NAME>;
USE SCHEMA <SCHEMA_NAME>;

CREATE TABLE <TABLE_NAME> (
    id STRING,
    mls STRING,
    compass_property_id STRING,
    status STRING,
    price FLOAT,
    bedrooms INTEGER,
    bathrooms FLOAT,
    square_feet INTEGER,
    property_type STRING,
    year_built INTEGER,
    address_line_1 STRING,
    address_line_2 STRING,
    street_number STRING,
    street_name STRING,
    street_type STRING,
    pre_direction STRING,
    unit_type STRING,
    unit_number STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    latitude FLOAT,
    longitude FLOAT,
    full_address STRING,
    presented_by STRING,
    presented_by_first_name STRING,
    presented_by_middle_name STRING,
    presented_by_last_name STRING,
    presented_by_mobile STRING,
    brokered_by STRING,
    listing_office_id STRING,
    listing_agent_id STRING,
    email STRING,
    email_1 STRING,
    email_2 STRING,
    list_date DATE,
    pending_date DATE,
    scraped_date DATE,
    open_house STRING,
    oh_startTime STRING,
    oh_company STRING,
    oh_contactName STRING,
    page_link STRING
);

```
3. Create an ElasticSearch if you dont have any. Once created, create your deployment and create an index in the deployment called 'real_estate_index_map'. Example to create index:

```bash
   PUT /real_estate_index_map
{
  "mappings": {
    "properties": {
      "id": { "type": "keyword" },
      "mls": { "type": "keyword" },
      "compass_property_id": { "type": "keyword" },
      "status": { "type": "keyword" },
      "price": { "type": "float" },
      "bedrooms": { "type": "integer" },
      "bathrooms": { "type": "float" },
      "square_feet": { "type": "integer" },
      "property_type": { "type": "keyword" },
      "year_built": { "type": "integer" },
      "address_line_1": { "type": "text" },
      "address_line_2": { "type": "text" },
      "street_number": { "type": "keyword" },
      "street_name": { "type": "keyword" },
      "street_type": { "type": "keyword" },
      "pre_direction": { "type": "keyword" },
      "unit_type": { "type": "keyword" },
      "unit_number": { "type": "keyword" },
      "city": { "type": "keyword" },
      "state": { "type": "keyword" },
      "zip_code": { "type": "keyword" },
      "latitude": { "type": "float" },
      "longitude": { "type": "float" },
      "full_address": { "type": "text" },
      "presented_by": { "type": "keyword" },
      "presented_by_first_name": { "type": "keyword" },
      "presented_by_middle_name": { "type": "keyword" },
      "presented_by_last_name": { "type": "keyword" },
      "presented_by_mobile": { "type": "keyword" },
      "brokered_by": { "type": "keyword" },
      "listing_office_id": { "type": "keyword" },
      "listing_agent_id": { "type": "keyword" },
      "email": { "type": "keyword" },
      "email_1": { "type": "keyword" },
      "email_2": { "type": "keyword" },
      "list_date": { "type": "date", "format": "yyyy-MM-dd||epoch_millis" },
      "pending_date": { "type": "date", "format": "yyyy-MM-dd||epoch_millis" },
      "scraped_date": { "type": "date", "format": "yyyy-MM-dd||epoch_millis" },
      "open_house": { "type": "keyword" },
      "oh_company": { "type": "keyword" },
      "oh_contactName": { "type": "keyword" },
      "oh_startTime": { "type": "keyword" },
      "page_link": { "type": "keyword" }
    }
  }
}
```
5. Deploy AWS Lambda by going to AWS Lambda console and creating a new python function. Upload the lambda_function.py file as a .zip file.
6. Attach/add the AWS built-in layer for pandas (AWSSDKPandas-Python39).
7. For the remaining dependencies for lambda follow the below steps:
   - Create a directory in your local PC name it wahtever you want or for example 'data_pipeline_dependecies' and inside this directory create a empty directory and name it 'python'.
   - Now open windows powershell in your 'data_pipeline_dependecies' directory.
   - Run the below command for snowflake-python-connector dependency compatible with linux:
   ```bash
   pip install snowflake-connector-python --target python --platform manylinux2014_x86_64 --only-binary=:all: --implementation cp --python-version 3.9 --upgrade
   ```
   - Run the below command for exceptiongroup dependency:
   ```bash
   pip install exceptiongroup --target python .
   ```
   - Run the below command for elasticsearch dependency:
   ```bash
   pip install elasticsearch --target python --upgrade 
   ```
   - Run the below command for slugify dependency:
   ```bash
   pip install python-slugify --target python --upgrade 
   ```
8. Once all these commands are run, run the following commands to compress the dependencies folder into a zip file 'lambda_all_dependencies':
   ```bash
   Compress-Archive -Path python -DestinationPath lambda_all_dependencies.zip
   ```
10. Now go to your AWS lambda and create a custom layer by going into Layers -> Create Layer. Upload the zip file that you just created from your local PC 'lambda_all_dependencies.zip'. Select the necessary architecture and your python version.
11. Once custom layer is created, attach/add the custom layer into your lambda function.
12. For snowflake connection add the following environment variables with the respective values in your lambda function:
   SNOWFLAKE_USER=
   SNOWFLAKE_PASSWORD=
   SNOWFLAKE_ACCOUNT=
   ELASTIC_CLOUD_ID=
   ELASTIC_PASSWORD=
13. Add S3 trigger in your lambda function (ensure the event type selected for this should be 'All object create events'.
14. Now deploy and test by uploading a csv file in S3 bucket and verify the results in your snowflake DWH in you respective table.
15. Also in your ElasticSearch deployment go to Kibana and run the following to get your index data:
    GET real_estate_index_map/_search?


# Tech Stack Used

AWS Lambda – Serverless execution

Amazon S3 – File ingestion

Snowflake – Cloud data warehouse

Elasticsearch – Search & analytics

Python + Pandas – ETL transformations

boto3, snowflake.connector, elasticsearch-py – Integrations

