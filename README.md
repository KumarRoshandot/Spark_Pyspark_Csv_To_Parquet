# Spark_Csv_To_Parquet
Spark Use case for Optimized processing with files

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
