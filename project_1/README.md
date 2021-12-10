# SPARKIFY PostgreSQL ETL

The aim of this project was to create a Postgres database for a startup company "Sparkify" in order to streamline access to song and user data
for analytics purposes. The goal was to develop an ETL pipeline to port the data over from their current format in a JSON directory
to that of a more easily accessible and query-able format.

## Schema
    
The layout of this project is a Star Schema with one central Fact Table and branching Dimension Tables

# Fact Table

The Songplays table serves as the Fact Table

# Dimension Tables

The Users, Songs, Artists and Time tables serve as the Dimension Tables


### File descriptions

-- data: contains the song and log data in json files

-- create_tables.py: used to create/drop tables. Must be run before any testing of queries or ETL script

-- etl.ipynb: notebook to test and set-up the ETL pipeline for reading, processing and loading of data from song and log files

-- etl.py: executable script form of the etl notebook

-- README.md: README file with summary of the project and instructions on running the scripts

-- sql_queries.py: contains all the SQL queries to be run including table dropping, creation and record insertions

-- test.ipynb: notebook for testing successful table creations/drops and record insertions/deletions and test for errors


### How-to run

1. Open your OS/Environment's console or equivalent
2. Run the "create_tables.py" file with the command: python create_tables.py
3. Verify that the tables were correctly created by running the cells in the test.ipynb notebook
4. Close the test.ipynb connection by restarting/terminating the kernel
5. Run the "etl.py" file in the console with the command: python etl.py
6. Verify that the scrips were run successfully by running the cells in test.ipynb once again