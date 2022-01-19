# Project 3 - Data Warehouse

## Project Summary

This project is comprised of building a Data Warehouse structure using AWS Redshift.
The goal in mind is to provide the growing music streaming startup 'Sparkify' with tools to manage and manipulate
user and song data in their expanding database.

## Setup Instructions

1. Set up a Redshift Cluster on AWS and an IAMROLE with read access to S3

2. Enter the required information into the fields present in the 'dwh.cfg' file found in the root folder

3. Execute the 'create_tables.py' script in your terminal to initialize the database structure with the required tables

4. Insert the data into the tables by running the 'etl.py' script in the terminal to extract the data from the targetted S3 buckets, stage it in redshift and then insert it into the tables

## DB Schema

| Table             | Info             |
| ---               | ---              |
| staging_events    | event data staging table |
| staging_songs     | song data staging table |
| artists           | information on artist names and locations |
| songplays         | information about who listened to songs, when and where |
| songs             | information related to songs such as title, artist, duration etc. |
| times             | time-related information about the songplays |
| users             | information about Sparkify users |


### Staging Tables
* staging_events
* staging_songs

### Fact Table
* songplays

### Dimension Tables
* users
* songs
* artists
* times

## Example queries

Find all of an artists songs:
"""
SELECT songs.sond_id FROM songs
JOIN artists ON songplays.artist_id = artists.artist_id
WHERE artists.name = <ARTIST>
"""

Find all users that listen to a certain song:
"""
SELECT DISTINCT songplays.user_id FROM songplays
JOIN songs ON songplays.song_id = songs.song_id
WHERE songs.title = <SONG>
"""