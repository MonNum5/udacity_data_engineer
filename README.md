# Data Engineering Udacity Nanodegree

<img src="https://www.udacity.com/blog/wp-content/uploads/2020/07/Data-Engineer_Blog-scaled.jpeg"
     style="width: 800px; height: 450px; margin-right: 10px;" />

This repo contains the material (exercises, solutions, support material and solution) of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). Learn course teaches to design data models, build data warehouses and data lakes, automate data pipelines with [PostgreSQL](https://www.postgresql.org/), [Apache Cassandra](https://cassandra.apache.org/), [Apache Spark](https://spark.apache.org/), [Amazon Web Services](https://aws.amazon.com/) and [Apache Airflow](https://airflow.apache.org/). The course is divided into 5 sections with 6 projects with the last one beeing a capstone project. More details can be found below

## 1. Data Modeling
In this section we learn about data modeling, relational data modeling with PostgreSQL and non-relational data modeling with Apache Cassandra.

### Project 1: Data Modelling with PostgreSQL
For this project we build a data processing pipeline for the music start up sparkify. We extract transform and load (ETL) data or users from a 
json format and provide them as SQL in PostgreSQL tables for the analytics team.

[Link to Data Modelling with PostgreSQL](https://github.com/MonNum5/udacity_data_engineer/tree/master/data_modelling/project_data_modelling_w_postgres)

### Project 2: Data Modelling with Apache Cassandra
Similar to project 1 we build a ETL pipline for the music start up sparkify to provide data for analytics team and answer questions like details of a song during a particular session, information about songs played by a particular user and all users that listen to a particular song. This time the modeling is done with Apache Cassandra.

[Link to Data Modelling with Apache Cassandra](https://github.com/MonNum5/udacity_data_engineer/tree/master/data_modelling/02_relational_data_models)

## 2. Cloud Data Warehouses
In this section we learn about the concept of data warehouses and how to implement those concepts on Amazon Web Services (AWS).

### Project 3: Data Warehouses with Amazon Web Services
We apply the data modeling concepts from project 1 to the cloud and implement a ETL pipeline that extracts the sparkify data from an AWS S3 bucket, stages them in Redshift, transforms the data into dimensional tables for the analytics team.

[Link to Data Warehouses with Amazon Web Services](https://github.com/MonNum5/udacity_data_engineer/tree/master/data_warehousing)

## 3. Data Lakes with Spark
This section is about data processing with Spark, the concept of data lakes and the implementation of those concepts on AWS

### Project 4: Data Lake with Spark
In this project we build a data lake and a ETL pipline in Spark (Pyspark) that loads data from an AWS S3 bucket, processes the data into analytics tables and loads the tables back into S3.

[Link to Data Lake with Spark](https://github.com/MonNum5/udacity_data_engineer/tree/master/data_lakes_w_spark)

## 4. Data Pipelines with Airflow
We implement a Airflow pipeline that handles, schedules and logs our ETL processes to extract data from an AWS S3 bucket, stages the data on AWS Redshift and loads the data to the final data model in AWS redshift.
[Link to Data Pipelines with Airflow](https://github.com/MonNum5/udacity_data_engineer/tree/master/data_pipelines_w_airflow)

## 5. Capstone Project
We apply all the content learn in this course to extract transform and load data from various datasets with Spark about US immigration and load the data to a PostgreSQL data model.
[Link to Capstone Project](https://github.com/MonNum5/udacity_data_engineer/tree/master/capstone_project)