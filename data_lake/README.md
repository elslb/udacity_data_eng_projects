# Project 4 - Data Lake

## Project Summary

This project is comprised of transitioning Sparkify's data to a Data Lake structure using Apache Spark, AWS S3 and Python's spark library (pyspark).

## Setup Instructions

1. Create an IAMROLE with read and write access to S3

2. Enter the required credentials into the "dl.cfg" configuration file found in the root folder

3. Create your target S3 bucket

4. Enter your target S3 bucket credential into the output_data field of the main function in the "etl.py" script

5. Run the "etl.py" script in your terminal

## DB Schema

| Table            | Table Type        | Info              |
| ---              | ---               | ---               |
| songplays        | Fact table        | inforomation about who listened to songs, when and where |
| songs            | Dimension table   | information related to songs such as title, artist, duration, etc. |
| artists          | Dimension table   | information on artist names and locations |
| users            | Dimension table   | information about Sparkify users |
| times            | Dimension table   | time-related information about the songplays |