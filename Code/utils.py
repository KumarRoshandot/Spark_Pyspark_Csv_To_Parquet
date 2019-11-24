'''
This will provide all relevant utility data required for processing
'''

from pyspark.sql.types import StringType, StructField , StructType
import sys
import logging
import os
temp_loc = '/tmp/'


def get_logger():
    '''
    This will return a logger which is a file based handler can be found at /tmp/ location
    :return: logger
    '''
    logger_file = os.path.join(temp_loc,'logs','uservisit.log')
    ensure_dir(logger_file)
    logger = logging.getLogger('uservisit')
    logger.setLevel(logging.DEBUG)

    # create a file handler
    fh = logging.FileHandler(filename=logger_file,mode='w+')
    fh.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add formatter
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


def ensure_arguments(total_arg):
    '''
    :param total_arg: total number of arguments passed to main job
    :return: True or False
    '''
    if total_arg < 1:
        return False
    else:
        return True


def ensure_dir(loc):
    '''
    :param loc: the local file directory path with filename
    :Purpose: to make sure the directory exists before processing
    '''
    directory = os.path.dirname(loc)
    if not os.path.exists(directory):
        os.makedirs(directory)


def changeToParquet(spark, old_file_loc,logger):
    '''
    :param spark: spark sparksession instance
    :param old_file_loc: the raw csv hdfs location
    :param logger: logger to log
    :return: parquet file path location
    '''
    # taking out the filename.csv from string
    parquet_loc = str(old_file_loc).rsplit('/',1)[0]
    parquet_file = os.path.join(parquet_loc,'parquet_src_files')

    # Schema
    schema = [StructField('user',StringType()),
                   StructField('URL',StringType()),
                   StructField('visit_time',StringType())]
    data_schema = StructType(fields=schema)
    try:
        csv_data = spark.read.csv(old_file_loc,schema=data_schema,header=False)
        #csv_data.printSchema()
        #csv_data.show(10)
        csv_data.write.parquet(parquet_file,mode="overwrite")
        return parquet_file
    except Exception as e:
        logger.info("Exception in Parquet Conversion :- {}".format(str(e)))
        exit(1)

