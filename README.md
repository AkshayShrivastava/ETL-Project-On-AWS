
# ETL-Project-On-AWS

"Data Engineering ETL Project on AWS leveraging S3, Glue, Spark Notebook, and Redshift Serverless. This repository contains the codebase and configurations for an end-to-end data pipeline, encompassing data extraction, transformation, and loading processes. Utilizing AWS Glue for scalable ETL jobs, Spark Notebooks for data processing, and S3 for storage, this project showcases best practices in modern data engineering. Redshift Serverless is employed for efficient data warehousing, ensuring seamless analytics and insights generation. Explore this repository for comprehensive documentation, code samples, and deployment guides."

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/b6a0e44e-49b8-4991-864c-79e98589331a)

## What we gonna do step by step
1. [Create IAM Role for whole project](#create-ıam-role-for-whole-project)
2. [Create an S3 bucket and load data to the bucket from our local](#create-an-s3)
3. [Create AWS Glue database and table](#create-aws-glue-database-and-table)
4. [Create Glue Studio Notebook](#create-glue-studio-notebook)
5. [Transform data using Spark](#transform-data-using-spark)
6. [Create AWS Redshift Cluster](#create-aws-redshift-cluster)
7. [Load the transformed data from S3 to Redshift](#load-the-transformed-data-from-s3-to-redshift)


# PART-1-EXTRACT

![Slide1 1396](https://github.com/askintamanli/Data-Engineer-ETL-Project-Using-Spark-with-AWS-Glue/assets/63555029/d8aea115-9c32-42e7-bf9a-ffebf47e0230)

## 1.1  Firstly we should create an IAM Role for whole project.

Go to AWS IAM → Roles → Create Role

Use cases for other AWS services : Select Glue

Add permissions → Search and Select ‘AdministratorAccess’

Role name : ‘tosds-etl-project-admin-access-role’

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/8ab9d8a0-db25-4fd5-8cc5-8cde797d9e27)


## 2.1- We create a bucket in AWS S3.
Go to AWS S3 → Buckets → Create bucket

Bucket name: ‘tosds-etl-project’

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/dbcd0168-695d-4d2c-8c0a-74f455855281)

## 2.2- We create database folder.

AWS S3 → Buckets → ‘tosds-etl-project’ → Create folder

Folder name: ‘tosds-etl-project-database’

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/df8e3950-526f-4237-91eb-982005a62731)


## 2.3- We create 2 folder for raw data and transformed data.

AWS S3 → Buckets → ‘tosds-etl-project’ → ‘tosds-etl-project-database’ → 2 x Create folder

Folder name : ‘raw_data’

Folder name : ‘transformed_data’

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/ee9917ab-d27c-409a-809f-bfdc43505e74)


## 2.4- We upload our data from local to ‘raw_data’ bucket. You can get the data from this repo.

AWS S3 → Buckets → ‘tosds-etl-project’ → ‘tosds-etl-project-database’ → ‘raw_data’ → Upload → Add Files → ‘marketing_campaign.csv’

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/ddfd0158-dc39-466d-a4ed-76968b04b8d0)


Okay, everything looks good in our bucket. Now, we should create Glue database and table. And load data to table from AWS S3.

## 3.1- Firstly, we create a database.

Go to AWS Glue → Data Catalog → Databases → Add database

Database name: ‘tosds-etl-project-database’

Location: Copy S3 URI of ‘tosds-etl-project-database’ folder and paste it to location space.

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/46f3712a-6d52-486f-8e46-57512e3aa5b8)


## 3.2- We create a table in database we just created.

AWS Glue → Data Catalog → Databases → ‘etl-project-for-medium-database’ → Add tables using crawler

Crawler name: ‘tosds-etl-project-crawler’

Data source S3 path: Choose the ‘raw_data’ bucket

IAM role → Choose IAM role → ’tosds-etl-project-admin-access-role’

Target database → Choose the ‘tosds-etl-project-database’

Schedule : On demand

and Run the crwaler.

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/170df389-f710-4505-b768-703b2e1a033f)


## 3.3- Our crawler is successfully completed. Let’s check the table and schema of our table.

AWS Glue → Data Catalog → Databases → ‘tosds-etl-project-database’ → raw_data

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/5fd866eb-a8a1-44c9-bba4-e62c2800d94e)

We just created our table. Check the data types of columns of data. Everything looks good in our table.

# PART-2-TRANSFORM

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/3aa73e31-8b1b-4960-8d26-6887a318b9b0)

## 4- Firstly we create ETL Job in AWS Glue.

Go to AWS Glue → Data Integration and ETL → Interactive Sessions → Notebooks

Job name: ‘tosds-etl-project-job’

IAM Role → Choose the role that we created first episode of series → ‘’osds-etl-project-admin-access-role’

Kernel : Spark

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/91820535-6d02-4ee1-8c0a-bcaa04499ed9)


## 5.1 Okay, let’s write some PySpark code for transform the data. You can get the source "etl-project-transform-data.ipynb" file in this repo.

1. For set up and start your interactive session.
   ```
    %idle_timeout 2880
    %glue_version 3.0
    %worker_type G.1X
    %number_of_workers 5

    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    ```
2. Create a DynamicFrame from a table in the AWS Glue Data Catalog and display its schema.
    ```
    dyf = glueContext.create_dynamic_frame.from_catalog(database=''tosds-etl-project-database',            table_name='raw_data')
    dyf.printSchema()
    ```
3. Convert the DynamicFrame to a Spark DataFrame and display a sample of the data.
    ```
    df = dyf.toDF()
    df.show()
    ```
4. Drop columns that we don't need it.
    ```
    df = df["id","year_birth","education","marital_status","income","dt_customer"]
    df.show()
    ```
5. Check NaN values for each column.
    ```
    from pyspark.sql.functions import *
    df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns]).show()
    ```
6. There are 24 NaN values in "income" column. Let's fill NaN values with mean.
    ```
    # Calculate the mean value of the column
    mean_value = df.select(mean(col('income'))).collect()[0][0]

    # Fill missing values with the mean value
    df = df.fillna(mean_value, subset=['income'])

    # Check
    df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns]).show()
    ```
7. Write the data to our S3 Bucket named "transformed_data" as csv.
    ```
    df.write \
      .format("csv") \
      .mode("append") \
      .option("header", "true") \
      .save("s3://tosds-etl-project/tosds-etl-project-database/transformed-data/")
    ```
8. Write the data to our S3 Bucket named "transformed_data" as json.
     ```
     df.write \
      .format("json") \
      .mode("append") \
      .save("s3://tosds-etl-project/tosds-etl-project-database/transformed-data/")
     ```



## 5.2 Let’s check our ‘transformed_bucket’.

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/f71c5644-2242-4e4e-b43c-1e1f6d74aad3)

Everything looks good. Select the one of them and download. You will see the results.

# PART-3-LOAD

![Slide1](https://user-images.githubusercontent.com/63555029/228977183-c3091fb1-6e57-4608-bf88-d24807af46bd.jpg)

## 6.1 We should create another IAM Role for Redshift.

Go to AWS IAM → Roles → Create Role

Use cases for other AWS services : Select Redshift - Customizable

Add permissions → Search and Select 'AdministratorAccess'

Role name : 'tosds-etl-project-redshift-iam-role'

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/b0b8fa34-cda9-4970-b47f-06438d098e06)



## 6.2- Let's kick-off AWS Redshift Serverless.

Go to AWS Redshift Serverless


![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/17259625-9fb8-4bc7-b02a-00b151c186d2)



## 6.3 We kicked off the cluster. Let's view our database with editor.

AWS Redshift Serverless →  Query Data v2

![2](https://user-images.githubusercontent.com/63555029/228977819-75df9364-b1da-47ad-a744-ead14f27b940.png)

'dev' and 'sample_data_dev' are database names. You can create notebook or editor page and you can run SQL codes.


## 7.1 Everything looks good. Let's load transformed data from s3 to 'dev' database. Firstly, we should create a table. Let's write some SQL for create table.

   ```
   CREATE TABLE etl_project_transformed_data_table(
   "id" INTEGER NULL,
   "year_birth" INTEGER NULL,
   "education" VARCHAR NULL,
   "marital_status" VARCHAR NULL,
   "income" INTEGER NULL,
   "dt_customer" DATE NULL
   ) ENCODE AUTO;
   ```
![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/7ddc4d0c-d17e-4507-90e1-635ffc051606)


## 7.2 Now, load the data.

Copy the S3 URI of transformed data which csv, paste it to 'from' field

Copy the ARN of IAM Role 'tosds-etl-project-redshift-iam-role', paste it IAM_ROLE field.

   ```
COPY tosds_etl_project_transformed_data_table
FROM 's3://etl-project-for-medium/tosds-etl-project-database/transformed_data/part-00000-6429f588-c5f4-4f6e-88df-b8bd3506113e-c000.csv'
IAM_ROLE 'arn:aws:iam::835769464848:role/tosds-etl-project-redshift-iam-role'
IGNOREHEADER 1
DELIMITER ',';

   ```
## 7.3 Let's check our table.

   ```
   SELECT * FROM tosds_etl_project_transformed_data_table
   ```
   
![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/49cd5a83-d860-44bc-8244-d95e3997de85)


## 7.4 Cool. Let's query the data.

   ```
   SELECT education, COUNT(id), AVG(income)
   FROM tosds_etl_project_transformed_data_table
   GROUP BY education
   ```
   
![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/09939b15-9047-47de-8a4c-e902ad516727)

You can even plot the graph using the data query v2

![image](https://github.com/AkshayShrivastava/ETL-Project-On-AWS/assets/32060525/4160b2ee-b2c3-45ca-b56e-919fe338e2c5)

## That's it. This is the end of this project. I really appreciate you for reading this series.
