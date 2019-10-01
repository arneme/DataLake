from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import hour, dayofmonth, weekofyear, month, year, dayofweek 
import pyspark.sql.types as t

def create_spark_session():
    """
    Create Spark session

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Data Lake Udacity project") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data, i.e. read all song data files into Spark and 
    transform into OLAP like song and artist (dimensions) tables.
    Store the song and artist tables into Spark parquet files.

    Parameters
    ----------
    spark : 
        The Spark session to use
    input_data : string
        The path to the input files (in S3)
    output_data : string
        The path to where the generated parquet files will be stored
    """
    # get filepath to song data file
    song_data_path = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'artist_id', \
                                 'title', 'year', 'duration') \
                        .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite") \
                    .parquet('s3://data-lake-sparkify/song_table')

    # extract columns to create artists table
    artists_table = song_df.select('artist_id', 'artist_name', \
                                'artist_location', 'artist_latitude', \
                                'artist_longitude') \
                            .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
                      .parquet('s3://data-lake-sparkify/artist_table')


def process_log_data(spark, input_data, output_data):
    """
    Process log data, i.e. read the log files into Spark and 
    transform into OLAP like user, time (dimensions) tables
    and songplay (fact) table. Store the fact and dimension tables
    into Spark parquet files.

    Parameters
    ----------
    spark : 
        The Spark session to use
    input_data : string
        The path to the input log files (in S3)
    output_data : string
        The path to where the generated parquet files will be stored
    """
    # get filepath to log data file
    song_data_path = input_data + 'song_data' + '/*/*/*/*.json'
    log_data = input_data + 'log_data' + '/*/*/*.json'

    event_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    event_df = event_df.filter(event_df.page == 'NextSong')

    # extract columns for users table    
    user_table = event_df.select('userId', 'firstName', 'lastName', \
                                 'gender', 'level', ) \
                         .dropDuplicates()
    
    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data + '/user_table')

    # create timestamp column from original timestamp column
    def get_ts (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    get_datetime = udf(lambda z: get_ts(z), t.TimestampType())

    datetime_df = event_df.select('ts').dropDuplicates() \
                          .withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = datetime_df.select(col('ts').alias('starttime'),
                   hour('datetime').alias('hour'), 
                   dayofmonth('datetime').alias('day'), 
                   weekofyear('datetime').alias('week'),
                   month('datetime').alias('month'),
                   year('datetime').alias('year'),
                   dayofweek('datetime').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite") \
                    .partitionBy("year", "month") \
                    .parquet('s3://data-lake-sparkify/time_table')

    # read in song data again to use for creating songplays table
    # Would probably have been a good idea to keep the dataframe
    # created in process_song_data, but the template seems to
    # dictate that it should be read again...

    song_df = spark.read.json(song_data_path)

    # extract columns from joined song and log datasets to create
    # songplays table 
    songplays_table = event_df.join(song_df, \
                                    event_df.artist == song_df.artist_name) \
                              .select('ts', 'userId', 'song_id', \
                                      'artist_id', 'level', 'sessionId',\
                                      'location', 'userAgent') \
                              .dropDuplicates()

    # write songplays table to parquet files
    # partitioned by year and month (which does not make sense since it is
    # not in table, I will use song_id and artist_id instead).

    songplays_table.write.mode("overwrite") \
                        .partitionBy("song_id", "artist_id") \
                        .parquet('s3://data-lake-sparkify/song_table')


def main():
    """
    Main program
    Will call the process_song_data and process_log_data functions
    to create all the OLAP oriented dimensions and fact tables and
    store them as parquet files for later use.

    Stops the spark session when done.
    """
    spark = create_spark_session()
    input_data = "s3n://udacity-dend/"
    # I have created a bucket named data-lake-sparkify
    # under my AWS account where all parquet files will be stored.
    output_data = "s3n://data-lake-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
