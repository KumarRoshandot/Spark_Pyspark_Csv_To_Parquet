# Spark_Csv_To_Parquet
- Spark Use case for Optimized processing with files
- This is a Pyspark Job Which Read files from AWS S3

---
## Work FLow 
- Executing the PYSPARK job on YARN.
- Place "User_visit_process.py" and "utils.py" at same location ( S3 , HDFS ), i considering s3 location
- The Main Program is User_visit_process.py which requires only one input source file hdfs location .
- HDFS LOCATION OF SOURCE CSV FILE :- /tmp/truecaller/data.csv or /tmp/truecaller/
- Run Below spark-submit command on a cluster.
	- ./bin/spark-submit --verbose\
                             --deploy-mode client \
                             --driver-memory 5G \
                              --executor-memory 10G \
                              --num-executors 2 \
                              --executor-cores 4 \
                               --conf spark.pyspark.python=/usr/bin/python3 \
                                --conf spark.pyspark.driver.python=/usr/bin/python3 \
                                --py-files s3://<BUCKETS>/TRUECALLER/utils.py \
                                 s3://<BUCKETS>/TRUECALLER/User_visit_process.py <HDFS LOCATION OF SOURCE CSV FILE>  

---
### Task divided for the above job.
- Since Source file is a CSV File and holds billion records , this requires some changes to lessen the execution overall time.
- So the raw CSV from HDFS is converted first to Parquet format (under 'parquet_src_files' folder ) without changing anything with respect to block size or partition, so that it can be easily be loaded via spark job and can be processed further with less execution time.
- The source parquet file is then been loaded in spark and evaluation of a user stay time on a URL done on it
- The output is again a parquet files created under 'parquet_output_files' which will hold the required data.
- logger is used here as a log file (uservisit.log) under /tmp/ location of unix master node

---
# Question:- 
Say we have a csv file in HDFS, which has three fields: a user ID, a URL and time when
user visited the URL (sample data below for a specific user).

     User1, xyzretail.com, 18:03:00
     User1, xyzretail.com/mobiles, 18:03:20
     User1, xyzretail.com/samusing-s8, 18:03:50
     User1, xyzretail.com, 18:04:00
     
Let us say this is data of users browsing xyzretail.com.

Main Question:

Please write a map-reduce/spark program that can convert the above data to another
data, which has a new derived field capturing the time the user stayed on that page. This field is
derived by taking the difference between times for current and next page for a user ID and then
associated with the current page. For the above example, the derived data should look like the
following (new field marked in bold):

	 User1, xyzretail.com, 18:03:00, 20s
	 User1, xyzretail.com/mobiles, 18:03:20, 30s
	 User1, xyzretail.com/samusing-s8, 18:03:50, 10s
	 User1, xyzretail.com, 18:04:00, NULL

Caveats:

		1. Data is potentially large with billions of rows and 100’s of millions of user IDs. So we
			want a scalable implementation.
		2. There is no guarantee of order in the data both for user ID’s and time.
