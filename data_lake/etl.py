import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as Stt, StructField as Sfd, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType as Tst

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initializes a Spark session or retrieves one if already present
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads the song_data JSONs from the given S3, processes them by extracting the data into
    song and artist tables in parquet format in the target output folder
    
    Paramters
    spark: spark session
    input_data: path to location of target json files
    output_data: path to S3 bucket where processed data will be stored
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    song_schema = Stt([
        Sfd("artist_id", Str()),
        Sfd("artist_latitude", Dbl()),
        Sfd("artist_location", Str()),
        Sfd("artist_longitude", Dbl()),
        Sfd("artist_name", Str()),
        Sfd("duration", Dbl()),
        Sfd("num_songs", Int()),
        Sfd("title", Str()),
        Sfd("year", Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table
    songs_cols = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(songs_cols).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.selectExpr(artists_cols).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Loads the log_data JSONs from the given S3, processes them by extracting the data into
    users, songplays and times tables in parquet format in the target output folder
    
    Paramters
    spark: spark session
    input_data: path to location of target json files
    output_data: path to S3 bucket where processed data will be stored
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    logs_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    logs_df = logs_df.filter(logs_df.page == "NextSong")

    # extract columns for users table
    users_cols = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = logs_df.selectExpr(users_cols).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, Tst())
    logs_df = logs_df.withColumn("timestamp", get_timestamp(logs_df.timestamp))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), Tst())
    logs_df = logs_df.withColumn("start_time", get_datetime(logs_df.timestamp))
    
    # extract columns to create time table
    times_table = logs_df.withColumn("hour", hour("start_time")) \
                         .withColumn("day", dayofmonth("start_time")) \
                         .withColumn("week", weekofyear("start_time")) \
                         .withColumn("month", month("start_time")) \
                         .withColumn("year", year("start_time")) \
                         .withColumn("weekday", dayofweek("start_time")) \
    
    times_table = times_table.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    times_table.write.partitionBy("year", "month").parquet(output_data + "times")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + "songs/*/*/*")
    artists_df = spark.read.parquet(output_data + "artists")
    
    songs_logs = logs_df.join(songs_df, (logs_df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.artist_name))

    # extract columns from joined song and log datasets to create songplays table     
    songplays_table = artists_songs_logs.join(
                      times_table, artists_songs_logs.timestamp == times_table.start_time, "left"
                      ).drop(artists_songs_logs.year)
    
    songplays_table = songplays_table.select(
        col("start_time"),
        col("userId").alias("user_id"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent"),
        col("year"),
        col("month")
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
