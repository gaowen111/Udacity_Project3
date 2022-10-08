import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window



config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    """
    Set up AWS keys and prepare for connecting to AWS.
    """

def create_spark_session():
    """
    Create spark connection.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """      
    - Read data from S3 for songs data.

    - Create songs_table according to songs dataset.
    
    - Output the songs_table to store in S3 on AWS.  
    
    - Create artists_table according to songs dataset.
    
    - Output the artists_table to store in S3 on AWS.  
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """      
    - Read data from S3 for song logs data.

    - Create users_table according to song logs dataset.
    
    - Output the users_table to store in S3 on AWS.  
    
    - Create time_table according to song logs dataset.
    
    - Output the time_table to store in S3 on AWS. 
    
    - Create songplays_table according to song logs dataset joining songs dataset.
    
    - Output the songplays_table to store in S3 on AWS. 
    """

    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE')
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x) / 1000)), TimestampType())
    df_time = df.select('ts').drop_duplicates() \
    .withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df_time.select('start_time').drop_duplicates() \
        .withColumn('hour', hour(col('start_time'))) \
        .withColumn('day', dayofmonth(col('start_time'))) \
        .withColumn('week', weekofyear(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('weekday', dayofweek(col('start_time')))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*"
    song_df = spark.read.json(song_data, mode='PERMISSIVE')
    
    # transfer ts to start time in songplays table
    df = df.withColumn('ts', get_timestamp(df.ts)) \
    .withColumnRenamed('ts','start_time')
    
    # Create temp view to be joined later
    df.createOrReplaceTempView("df_log_table")
    song_df.createOrReplaceTempView("df_song_table")

    # Preparation to add row number
    w = Window().orderBy(lit('A'))
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    select DISTINCT a.start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, 
    b.artist_location, a.userAgent, year(a.start_time) as year, month(a.start_time) as month
    from df_log_table a left join df_song_table b 
    on a.song = b.title''').withColumn("rowNum", row_number().over(w))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays/")



def main():
    """      
    - Create spark connection.

    - Set AWS S3 common path.
    
    - Process songs data and save data back to S3.  
    
    - Process song logs data and save data back to S3.    
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityprojectwen4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
