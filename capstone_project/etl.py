import os
import pandas as pd
import psycopg2
import pyspark
import configparser
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, to_date, monotonically_increasing_id


# AWS credential setup
config = configparser.ConfigParser()
config.read('cap.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
SRC_BUCKET = config['S3']['SRC_BUCKET']
OUTPUT_BUCKET = config['S3']['OUTPUT_BUCKET']


# Setting up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



# Setting up SparkSession initializer
def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
                        .enableHiveSupport().getOrCreate()
    return spark



def sas_to_pd_date(date):
    '''
    Changes the input date format to pandas dt format
    Input date must not be Null or None type
    '''
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

# adding udf from spark to the function
sas_to_pd_date_udf = udf(sas_to_pd_date, DateType())



def rename_cols(table, new_cols):
    '''
    Function for renaming columns to desired target format
    '''
    for old, new in zip(table.columns, new_cols):
        table = table.withColumnRenamed(old, new)
    return table


#### Data Processing Functions

def process_imm_data(spark, src_data, output_data):
    '''
    Processes immigration data and builds the 3 following tables:
    ft_imm - immigration fact table
    dt_imm_indiv - dimension table containing information about individual immigrants
    dt_imm_airline - dimension table 
    
    params:
        spark : SparkSession object
        src_data : AWS S3 endpoint for the bucket acting as the source of data
        output_data : AWS S3 endpoint for the bucket where data will be stored after processing
    '''
    
    
    logging.info("Immigration data processing initialized...")
    
    # reading data file
    imm_data = os.path.join(src_data + 'data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(imm_data)
    
    
    logging.info("Now processing immigration fact table 'ft_imm'")
   

    ft_imm = df_spark.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',
                             'arrdate', 'depdate', 'i94mode', 'i94visa')\
                    .distinct().withColumn('immigration_id', monotonically_increasing_id())
    
    # new columns for renaming ft_imm columns
    new_cols = ['cic_id', 'year', 'month', 'city_code', 'state_code', 'arr_date',
                'dep_date', 'mode_code','visa_code']
    ft_imm = rename_cols(ft_imm, new_cols)
    
    # transforming sas date format to pd dt
    ft_imm = ft_imm.withColumn('country', lit('United States'))
    ft_imm = ft_imm.withColumn('arr_date', sas_to_pd_date_udf(col('arr_date')))
    ft_imm = ft_imm.withColumn('dep_date', sas_to_pd_date_udf(col('dep_date')))
    
    # writing the table to parquet file
    ft_imm.write.mode("append").partitionBy('state_code').parquet(output_data + 'ft_imm')
    
    
    logging.info("Completed processing 'ft_imm'. Now processing 'dt_imm_indiv'")
    
    
    dt_imm_indiv = df_spark.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum')\
                            .distinct().withColumn('imm_indiv_id', monotonically_increasing_id())
    
    # new columns for renaming dt_imm_indiv columns
    new_cols = ['cic_id', 'citizenship', 'residency', 'birth_year', 'gender', 'ins_num']
    dt_imm_indiv = rename_cols(dt_imm_indiv, new_cols)
    
    # writing the table to parquet file
    dt_imm_indiv.write.mode("append").parquet(output_data + 'dt_imm_indiv')
    
    
    logging.info("Completed processing 'dt_imm_indiv'. Now processing 'dt_imm_airline'")
    
    
    dt_imm_airline = df_spark.select('cicid', 'airline', 'admnum', 'fltno', 'visatype')\
                            .distinct().withColumn('imm_airline_id', monotonically_increasing_id())
    
    # new columns for renaming dt_imm_airline columns
    new_cols = ['cic_id', 'airline', 'admin_number', 'flight_number', 'visa_type']
    dt_imm_airline = rename_cols(dt_imm_airline, new_cols)
    
    # writing the table to parquet file
    dt_imm_airline.write.mode("append").parquet(output_data + 'dt_imm_airline')
    
    
    
def SAS_label_parsing(spark, src_data, output_data):
    '''
    Creating a bridge table to link different country, city and state codes
    
    Params:
        spark : SparkSession object
        src_data : AWS S3 endpoint for the bucket acting as the source of data
        output_data : AWS S3 endpoint for the bucket where data will be stored after processing
    '''
    
    logging.info("Parsing SAS code labels...")
    fname = os.path.join(src_data + 'I94_SAS_Labels_Descriptions.SAS')
    with open(fname) as f:
        i94_data = f.readlines()
        
    
    country_codes = {}
    for countriies in i94_data[10:298]:
        # we will have to split on = to separate the code from the country
        line = countries.split('=')
        code, country = line[0].strip(), line[1].strip().strip("''")
        country_codes[code] = country
    spark.createDataFrame(country_codes.items(), ['code', 'country'])\
                        .write.mode("append").parquet(output_data + 'country_codes')
    
    city_codes = {}
    for cities in i94_data[303:962]:
        line = cities.split('=')
        code, city = line[0].strip("\t").strip().strip("''"), line[1].split(",")[0].strip("\t").strip("''")
        city_codes[code] = city
    spark.createDataFrame(city_codes.items(), ['code', 'city'])\
                        .write.mode("append").parquet(output_data + 'city_codes')
    
    state_codes = {}
    for states in i94_data[982:1036]:
        line = states.split('=')
        code, state = line[0].strip=("\t").strip("''"), line[1].strip().strip("''")
        state_codes[code] = state
    spark.createDataFrame(state_codes.items(), ['code', 'state'])\.
                        .write.mode("append").parquet(output_data + 'state_codes')
    
    
def process_temp_data(spark, src_data, output_data):
    '''
    Processes temperature data to produce the temperature dimension table:
    
    dt_temp_us - dimension table containing information about the average monthly
                 temperatures in the United States
                 
    params:
        spark : SparkSession object
        src_data : AWS S3 endpoint for the bucket acting as the source of data
        output_data : AWS S3 endpoint for the bucket where data will be stored after processing
    '''
    
    logging.info("Temperature data processing initialized...")
    
    
    # reading data file
    temp_data = os.path.join(src_data + 'GlobalLandTemperaturesByCity.csv')
    df_spark = spark.read.csv(temp_data, header = True)
    
    # setting country to only U.S.
    df_spark = df_spark.where(df_spark['Country'] == 'United States')
    
    dt_temp_us = df_spark.select('dt', 'AverageTemperature', 'AverageTemperatureUncertainty',
                                 'City', 'Country', 'Latitude', 'Longitude').distinct()
    
    # new columns for renaming dt_temp_us columns
    new_cols = ['dt', 'avg_temp', 'avg_temp_uncertainty', 'city', 'country', 'latitude', 'longitude']
    dt_temp_us = rename_cols(dt_temp_us, new_cols)
    
    dt_temp_us = dt_temp_us = dt_temp_us.withColumn('dt', to_date(col('dt')))
    dt_temp_us = dt_temp_us = dt_temp_us.withColumn('year', year(dt_temp_us['dt']))
    dt_temp_us = dt_temp_us = dt_temp_us.withColumn('month', month(dt_temp_us['dt']))
    
    # writing the table to parquet file
    dt_temp_us.write.mode("append").parquet(output_data + 'dt_temp_us')
    
    

def process_demo_data(spark, src_data, output_data):
    '''
    Processes demographic data for U.S. cities to produce a dimension table:
    
    dt_demo - dimension table containing information about the demographics of U.S. cities
    
    params:
        spark : SparkSession object
        src_data : AWS S3 endpoint for the bucket acting as the source of data
        output_data : AWS S3 endpoint for the bucket where data will be stored after processing
    '''
    
    logging.info("Demographic data processing initialized...")
    
    
    # reading data file
    demo_data = os.path.join(src_data + 'us-cities-demographics.csv')
    df_spark = soark.read.csv(demo_data, header=True, sep=';')
    
    dt_demo = df_spark.select('City', 'State', 'Median Age', 'Male Population',
                              'Female Population', 'Total Population', 'Number of Veterans',
                              'Foreign-born', 'Average Household Size', 'State Code', 'Race')\
                              .distinct().withColumn('demo_id', monotonically_increasing_id())
    
    # new columns for renaming dt_demo columns
    new_cols = ['city', 'state', 'median_age', 'male_population',
                'female_population', 'total_population', 'num_of_veterans',
                'foreign_born', 'avg_household_size', 'state_code', 'race']
    dt_demo = rename_cols(dt_demo, new_cols)
    
    # writing the table to parquet file
    dt_demo.write.mode("append").parquet(output_data + 'dt_demo')
        


#### Data Quality Check Functions

def perform_data_quality_check_1(output_data):
    for directory in output_data.iterdit():
        if directory.is_dir():
            
            # printing the schema of the generated tree to compare with expected schema
            path = str(directory)
            df_spark = spark.read.parquet(path)
            print("Displaying directory tree for: " + path.split("/")[-1])
            schema = df_spark.printSchema()
            return schema
    

    
def perform_data_quality_check_2(tables):
    '''
    Programmatically checks for presence of data in each table
    '''
    
    for table in tables:
        if table.count() == 0
        raise ValueError(f"Something went wrong! {table} is empty!".format)
    
    
    
def main():
    spark = create_spark_session()
    src_data = SRC_BUCKET
    output_data = OUTPUT_BUCKET
    tables = [ft_imm, dt_imm_indiv, dt_imm_airline, dt_demo,
              dt_temp_us, state_codes, city_codes, country_codes]
    
    process_imm_data(spark, src_data, output_data)
    SAS_label_parsing(spark, src_data, output_data)
    process_temp_data(spark, src_data, output_data)
    process_demo_data(spark, src_data, output_data)
    perform_data_quality_check_1(output_data)
    perform_data_quality_check_2(tables)
    logging.info("Operations completed!")
    

if __name__ == '__main__':
    main()