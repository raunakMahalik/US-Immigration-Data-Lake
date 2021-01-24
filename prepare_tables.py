# Library imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, udf, year, \
month, weekofyear, dayofmonth, monotonically_increasing_id
import datetime as dt

# Path to parquet files on s3
AIRPORTS_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/airports.parquet'
DEMOGRAPHICS_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/demographics.parquet'
I94_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/i94_fact.parquet'
IMMIGRANTS_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/immigrants.parquet'
ARRIVALS_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/arrivals.parquet'
DEPARTURES_PARQUET_PATH = 's3a://raunak-udacity-capstone-bucket/departures.parquet'

def create_airports_dim_table(spark_df, spark):
    """Description:
       Creates the airport dimension table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Use SQL query to create airport dimension table from
           the view.
        3. Write the dimesion table to a parquet file.
        4. Return the dimension table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            airports_dim_table {spark dataframe}: airport dimension table.
    """
    spark_df.createOrReplaceTempView("temp_airport_table")
    airports_dim_table = spark.sql("""SELECT \
                                        ident AS airport_ident, \
                                        name AS airport_name, \
                                        type AS airport_type, \
                                        elevation_ft AS airport_elevation, \
                                        iata_code AS airport_iata_code, \
                                        gps_code AS airport_gps_code, \
                                        municipality AS airport_mun, \
                                        iso_country AS airport_country, \
                                        iso_region AS airport_region, \
                                        SPLIT(coordinates,',')[0]  AS latitude, \
                                        SPLIT(coordinates,',')[1]  AS longitude \
                                  FROM \
                                        temp_airport_table""").dropDuplicates()
    
    # Write to parquet file
    airports_dim_table.write.mode('overwrite').partitionBy("airport_country").parquet(AIRPORTS_PARQUET_PATH)
    
    return airports_dim_table


def create_demographics_dim_table(spark_df, spark):
    """Description:
       Creates the demographics dimension table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Use SQL query to create demographics dimension table from
           the view.
        3. Write the dimesion table to a parquet file.
        4. Return the dimension table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            demographics_dim_table {spark dataframe}: demographics dimension table.
    """
    spark_df.createOrReplaceTempView("temp_demographics_table")
    demographics_dim_table = spark.sql("""SELECT \
                                            monotonically_increasing_id() AS unique_id, \
                                            City AS city, \
                                            State AS state, \
                                            'State Code' AS state_code, \
                                            Race AS race, \
                                            Count AS count, \
                                            'Median Age' AS median_age, \
                                            'Male Population' AS male_population, \
                                            'Female Population' AS female_population, \
                                            'Total Population' AS total_population, \
                                            'Number of Veterans' AS total_veterans, \
                                            'Foreign-born' AS foreign_born, \
                                            'Average Household Size' AS avg_household_size \

                                      FROM \
                                            temp_demographics_table""").dropDuplicates()
    
    # Write to parquet file
    demographics_dim_table.write.mode('overwrite').partitionBy("city").parquet(DEMOGRAPHICS_PARQUET_PATH)
    
    return demographics_dim_table


def create_i94_fact_table(spark_df, spark):
    """Description:
       Creates the i94 fact table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Use SQL query to create i94 fact table from
           the view.
        3. Write the fact table to a parquet file.
        4. Return the fact table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            i94_fact_table {spark dataframe}: i94 fact table.
    """
    spark_df.createOrReplaceTempView("temp_i94_table")
    i94_fact_table = spark.sql("""SELECT \
                                    cicid, \
                                    i94yr AS year, \
                                    i94port AS port_of_entry, \
                                    arrdate AS arr_date, \
                                    i94mode AS mode, \
                                    i94addr AS arrival_state, \
                                    depdate AS dep_date, \
                                    dtadfile AS dtad_file, \
                                    entdepa AS arr_flag, \
                                    entdepd AS dep_flag, \
                                    matflag AS match_flag, \
                                    airline, \
                                    admnum AS adm_num, \
                                    fltno AS flight_num, \
                                    i94mon AS i94_month \

                              FROM \
                                    temp_i94_table""").dropDuplicates()
    
    # udf to convert arrival date to datetime type
    conv_to_datetime = udf(lambda i: (dt.datetime(1960, 1, 1).date() + dt.timedelta(i)).isoformat() if i else None)
    
    # Change dates to datetime format
    i94_fact_table = i94_fact_table.withColumn("dep_date", conv_to_datetime(i94_fact_table.dep_date))
    i94_fact_table = i94_fact_table.withColumn("arr_date", conv_to_datetime(i94_fact_table.arr_date))
    
    # Write to parquet file
    i94_fact_table.write.mode('overwrite').partitionBy("year").parquet(I94_PARQUET_PATH)

    return i94_fact_table


def create_immigrants_dim_table(spark_df, spark):
    """Description:
       Creates the immigrants dimension table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Use SQL query to create immigrants dimension table from
           the view.
        3. Write the dimesion table to a parquet file.
        4. Return the dimension table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            immgs_dim_table {spark dataframe}: immigrants dimension table.
    """
    spark_df.createOrReplaceTempView("temp_i94_table")
    immgs_dim_table = spark.sql("""SELECT \
                                    cicid, \
                                    biryear AS birth_year, \
                                    i94bir AS age, \
                                    gender AS sex, \
                                    i94cit AS birth_country, \
                                    i94res AS res_country, \
                                    visatype AS visa_type, \
                                    CASE i94visa
                                        WHEN '1.0' THEN 'Business'
                                        WHEN '2.0' THEN 'Pleasure'
                                        WHEN '3.0' THEN 'Student'
                                        ELSE 'Unclassified'
                                    END AS visa_desc, \
                                    dtaddto AS valid_till_date \
                              FROM \
                                    temp_i94_table""").dropDuplicates()
    
    # Write to parquet file
    immgs_dim_table.write.mode('overwrite').partitionBy("birth_country").parquet(IMMIGRANTS_PARQUET_PATH)

    return immgs_dim_table


def create_arrivals_dim_table(i94_spark_df):
    """Description:
       Creates the arrivals dimension table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Extract year, month, week and day details from the date field
           and populate the same into the arrivals dimension table.
        3. Write the dimesion table to a parquet file.
        4. Return the dimension table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            arrivals_dim_table {spark dataframe}: arrivals dimension table.
    """
    # udf to convert arrival date to datetime type
    conv_to_datetime = udf(lambda i: (dt.datetime(1960, 1, 1).date() + dt.timedelta(i)).isoformat() if i else None)
    
    arrivals_dim_table = i94_spark_df.select(['arrdate']).withColumn("arrdate", \
                                                                    conv_to_datetime(i94_spark_df.arrdate))
    arrivals_dim_table = arrivals_dim_table.withColumn('year', year('arrdate'))
    arrivals_dim_table = arrivals_dim_table.withColumn('month', month('arrdate'))
    arrivals_dim_table = arrivals_dim_table.withColumn('week', weekofyear('arrdate'))
    arrivals_dim_table = arrivals_dim_table.withColumn('day', dayofmonth('arrdate'))
    arrivals_dim_table = arrivals_dim_table.dropDuplicates()
    
    # Write to parquet file
    arrivals_dim_table.write.mode('overwrite').partitionBy("year").parquet(ARRIVALS_PARQUET_PATH)
    
    return arrivals_dim_table


def create_departures_dim_table(i94_spark_df):
    """Description:
       Creates the departures dimension table by doing the following:
        1. Create a view of the input spark dataframe.
        2. Extract year, month, week and day details from the date field
           and populate the same into the departures dimension table.
        3. Write the dimesion table to a parquet file.
        4. Return the dimension table.
    
        Arguments:
            spark_df {pySpark dataframe}: Cleaned spark dataframe.
            spark{object}: Spark object
            
        Returns:
            departures_dim_table {spark dataframe}: departures dimension table.
    """
    # udf to convert arrival date to datetime type
    conv_to_datetime = udf(lambda i: (dt.datetime(1960, 1, 1).date() + dt.timedelta(i)).isoformat() if i else None)
    
    departures_dim_table = i94_spark_df.select(['depdate']).withColumn("depdate", conv_to_datetime(i94_spark_df.depdate))
    departures_dim_table = departures_dim_table.withColumn('year', year('depdate'))
    departures_dim_table = departures_dim_table.withColumn('month', month('depdate'))
    departures_dim_table = departures_dim_table.withColumn('week', weekofyear('depdate'))
    departures_dim_table = departures_dim_table.withColumn('day', dayofmonth('depdate'))
    departures_dim_table = departures_dim_table.dropDuplicates()
    
    # Write to parquet file
    departures_dim_table.write.mode('overwrite').partitionBy("year").parquet(DEPARTURES_PARQUET_PATH)
        
    return departures_dim_table