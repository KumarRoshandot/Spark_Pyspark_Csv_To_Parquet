'''
This Program will evaluate a user stay duration on a URL and save it on a
hdfs location as parquet format
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from utils import *

# This block size is considering hdfs block size
block_size = 128 * 1024

'''
2 config properties , rest configuration can be passed from outside while calling spark-submit
1) the URL string length can become large, so setting it maxtoStringFields
2) the parquet block size, this is because to save the raw csv as it is to parquet with 
   similar partition...This is done as the data can be in billions and csv is text format which 
   will can not be parsed as efficiently as a binary format, So csv is slower compared to parquet.   
'''
spark = SparkSession.\
            builder.\
            appName("User_visits_URL"). \
            config('spark.sql.debug.maxToStringFields', 2000).\
            config('spark.hadoop.parquet.block.size', block_size).\
            getOrCreate()

logger = get_logger()


def users_stay_check(file_loc):
    '''
    :param file_loc: source parquet file location
    :return: nothing , it creates output which will have user stay duartion info in parquet format
    '''
    try:
        df = spark.read.parquet(file_loc)
        df = df.withColumn("duration",unix_timestamp(df["visit_time"],"HH:mm:ss"))
        df = df.orderBy(df["user"],df["duration"])
        df.createOrReplaceTempView("user_visit_raw")
        sql = '''
        Select 
        *,
        cast(((lead(duration) over (partition by user order by duration) - duration)) as string)||'s' as time_stayed 
        from 
        user_visit_raw    
        '''
        df = spark.sql(sql)
        parquet_loc = str(file_loc).rsplit('/', 1)[0]
        parquet_file = os.path.join(parquet_loc, 'parquet_output_files')

        df.write.\
            parquet(parquet_file,mode="overwrite")
        #df.show(10,False)

    except Exception as e:
        logger.info("Exception in user stay check process :- {}".format(str(e)))
        exit(1)


if __name__ == "__main__":
    # making sure we pass relevant argument e.g.. csv source location
    # e.g.. </tmp/truecaller/srcfiles/> or </tmp/truecaller/srcfiles/data.csv>
    ensure_arg = ensure_arguments(len(sys.argv) - 1)
    if ensure_arg is False:
        logger.info("Invalid Arguments , Provide minimum 1 argument")
        exit(1)
    
    input_file_loc = sys.argv[1]
    logger.info("Inputfile for processing {}".format(input_file_loc))

    # converting raw csv as it is in parquet form for fast execution for main processing
    parquet_file = changeToParquet(spark,input_file_loc,logger)
    logger.info('After Converting CSV to Parquet :- {}'.format(parquet_file))
    logger.info('Enjoy with parquet')

    # finding out user's stay on a URL
    users_stay_check(parquet_file)

    # below code, Logger log file can be move to hdfs or s3 location





