
# Purpose
This project is aiming to analyze the data that a startup called Sparkify has been collected on songs and user activity on their new music streaming app.
The main focus is to understand what songs users are listening to. We are using AWS S3 as the storage, due to it is on cloud, so it is safe, trustable, scalable, and have good performance. And we are using spark (AWS EMR) as the data handling technology, as spark makes big data handling more effecient and fast.

# How to run the scripts

- ## Pre-condition
Need to create an EMR cluster, a S3 bucket to store the output data, and an IAM user allows notebook to read/ write data to S3.

- ## Steps
1. Configure AWS user credentials in dl.cfg file.
2. Then run the python file: etl.py

# Files in the repository
1. dl.cfg: To put AWS user credentials that we need to connect to AWS.
2. etl.py: Copy data from S3 bucket and do data transformation, and write to another S3 bucket to store, using spark to handle data.

# Dataset and ETL pipeline
There are 2 input datasets from S3 as songs data and song logs data. The code use spark to read from S3 and do the data transformation, also to write to another S3 bucket for the output data. The outcome shall be 5 different datasets stored on the other S3 bucket, and they are songs, artists, time, users and songplays.
