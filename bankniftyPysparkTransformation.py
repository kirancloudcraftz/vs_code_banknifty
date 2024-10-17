from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DoubleType, TimestampType
from pyspark.sql.types import DecimalType, DateType

from pyspark.sql.functions import split, to_timestamp, concat_ws, to_date, date_format

from google.cloud import storage
from google.cloud import bigquery

from datetime import datetime

#Storing the bucket folders to validate before execution

client = storage.Client()
bucket_name = 'testingbanknifty'
bucket = client.get_bucket(bucket_name)

# List of prefixes (folders) in the bucket

folder_name = 'BANKNIFTY/2020/'
blobs = bucket.list_blobs(prefix=folder_name)

# Initialize a list to store folder names
folders = []

# Loop through blobs and check for folders
for blob in blobs:
    folder_name = blob.name.split('/')[2]  # Get the 2nddeep-level folder name
    if folder_name not in folders:
        folders.append(folder_name)

#now use the folders in spark csv read files to avoid error in job.


# Sparksession started
spark = SparkSession.builder.appName("TestingGCStoBigqueryTransfer")\
        .config("spark.jars.packages", "com.google.cloud.spark:spark-3.1-bigquery-0.41.0")\
        .getOrCreate()
        
# For POC keeping this as hard code will work on this.
for folder in folders:

    csv_path = "gs://testingbanknifty/BANKNIFTY/2020/" + folder +"/"

    # Creating a spark dataframe with schema and considering header is present in every file
    df = spark.read.option("header","True").option("inferschema", "True").csv(csv_path)

    # split the date time column to yearmonthdate compbine and hour minutes seconds combinely so that we can use for storing purpose and filtering date column

    df = df.withColumn("YMD", split(df["Date Time"], " ").getItem(0))\
            .withColumn("HH:MM:SS",split(df["Date Time"], " ").getItem(1))

    df = df.withColumn("Year",split(df["YMD"], "-").getItem(0))\
            .withColumn("Month",split(df["YMD"], "-").getItem(1))\
            .withColumn("Day",split(df["YMD"], "-").getItem(2))

    # Now converting our splitted data into timestamp datatype

    df = df.withColumn("DateTime", to_timestamp(concat_ws(" ",concat_ws("-",df["Day"],df["Month"],df["Year"]),df["HH:MM:SS"]),
                                                 'dd-MM-yyyy HH:mm:ss'))

    #converting ExpiryDate & ExpiryTime to date & string datatype
    df = df.withColumn("Year1",split(df["ExpiryDate"], "-").getItem(0))\
            .withColumn("Month1",split(df["ExpiryDate"], "-").getItem(1))\
            .withColumn("Day1",split(df["ExpiryDate"], "-").getItem(2))

    df = df.withColumn("ExpiryDate", to_date(concat_ws(" ",concat_ws("-",df["Day1"],df["Month1"],df["Year1"])), "dd-MM-yyyy"))\
            .drop("Year1","Month1","Day1")\
            .withColumn("ExpiryTime", df["ExpiryTime"].cast(StringType()))

    # split the date time column to yearmonthdate compbine and hour minutes seconds combinely so that we can use for storing purpose and filtering date column
    # and converting our splitted data into timestamp datatype

    df = df.withColumn("YMD1", split(df["ExpiryDateTime"], " ").getItem(0))\
            .withColumn("HH:MM:SS1",split(df["ExpiryDateTime"], " ").getItem(1))

    df = df.withColumn("Year1",split(df["YMD1"], "-").getItem(0))\
            .withColumn("Month1",split(df["YMD1"], "-").getItem(1))\
            .withColumn("Day1",split(df["YMD1"], "-").getItem(2))


    df = df.withColumn("ExpiryDateTime", to_timestamp(concat_ws(" ",concat_ws("-",df["Day1"],df["Month1"],df["Year1"]),df["HH:MM:SS1"]),
                                                 'dd-MM-yyyy HH:mm:ss'))

    df = df.drop("YMD1","HH:MM:SS1","Year1","Month1","Day1")

    df = df.withColumn("ExchToken", df["ExchToken"].cast(IntegerType()))\
            .withColumn("BidPrice", df["BidPrice"].cast(FloatType()))\
            .withColumn("AskPrice",df["AskPrice"].cast(FloatType()))\
            .withColumn("Time_to_expire",df["Time_to_expire"].cast(StringType()))\
            .withColumn("Premium",df["Premium"].cast(StringType()))\
            .withColumn("Delta",df["Delta"].cast(StringType()))\
            .withColumn("Theta",df["Theta"].cast(StringType()))\
            .withColumn("Gamma",df["Gamma"].cast(StringType()))\
            .withColumn("Vega",df["Vega"].cast(StringType()))\
            .withColumn("Sigma",df["Sigma"].cast(StringType()))\
            .withColumn("bid_ask_spread",df["bid_ask_spread"].cast(StringType()))\
            .withColumn("mid_price",df["mid_price"].cast(FloatType()))\
            .withColumn("bid_ask_move",df["bid_ask_move"].cast(StringType()))\
            .withColumn("bid_plus",df["bid_plus"].cast(StringType()))\
            .withColumn("ask_minus",df["ask_minus"].cast(StringType()))\
            .withColumn("BidQty", df["BidQty"].cast(IntegerType()))\
            .withColumn("AskQty", df["AskQty"].cast(IntegerType()))\
            .withColumn("TTq", df["TTq"].cast(IntegerType()))\
            .withColumn("LTP", df["LTP"].cast(IntegerType()))\
            .withColumn("TotalTradedPrice", df["TotalTradedPrice"].cast(IntegerType()))\
            .withColumn("Strike", df["Strike"].cast(IntegerType()))\
            .withColumn("Spot", df["Spot"].cast(IntegerType()))\
            .withColumn("Intrinsic_value", df["Intrinsic_value"].cast(IntegerType()))\
            .withColumn("price_problem", df["price_problem"].cast(BooleanType()))\
            .withColumn("is_tradable", df["is_tradable"].cast(BooleanType()))\
            .withColumn("Year", df["Year"].cast(IntegerType()))\
            .withColumn("Month", df["Month"].cast(IntegerType()))
            
    # Selecting only those columns which are necessary to store with considering camelcase for column names
    df = df.select(df["DateTime"].alias("ExchDateTime"),
                   "ExchToken",
                   "BidPrice",
                   "BidQty",
                   "AskPrice",
                   "AskQty",
                   df.TTq.alias("TTQ"),
                   "LTP",
                   "TotalTradedPrice",
                   "Instrument",
                   "ExpiryDate",
                   "ExpiryTime",
                   "Strike",
                   "Type",
                   "ExpiryDateTime",
                   "Spot",
                   df.Time_to_expire.alias("TimeToExpire"),
                   "Premium",
                   "Delta",
                   "Theta",
                   "Gamma",
                   "Vega",
                   "Sigma",
                   df.bid_ask_spread.alias("BidAskSpread"),
                   df.mid_price.alias("MidPrice"),
                   df.Intrinsic_value.alias("IntrensicValue"),
                   df.bid_ask_move.alias("BidAskMove"),
                   df.price_problem.alias("PriceProblem"),
                   df.is_tradable.alias("IsTradable"),
                   df.bid_plus.alias("BidPlus"),
                   df.ask_minus.alias("AskMinus"),
                   "YMD","Year","Month")

            
    print("Mapping & Datatypes change is completed")
    
    # Fetch unique dates from a BigQuery table to avoid duplication, store them in a list for comparison.

    # Initialize client 
    client = bigquery.Client()
    
    # define query to fetch unique dates for exch date column 
    query = (
    'SELECT DISTINCT(DATE(ExchDateTime)) as unique_dates FROM eis-global-dev.BankNiftyTest.BankNifty')
    
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    
    results = client.query(
    query, job_config=job_config
    )
    
    # store the output from results into list for comparison.
    output_list = [row['unique_dates'] for row in results]
    bq_exch_dates = []
    for i in output_list:
        formated_date = i.strftime("%Y-%m-%d")
        bq_exch_dates.append(formated_date)
        
    # Fetching unique exchange dates from current dataframe
    df_exch_unique_dates = df.select(to_date(df["ExchDateTime"]).alias("Date")).distinct().collect()
    
    # storing it into list
    df_exch_list = [i['Date'].strftime("%Y-%m-%d") for i in df_exch_unique_dates]
    
    for i in df_exch_list:
        if i in bq_exch_dates:
            print(f"Exchange date {i} is already present in the bigquery table")
        else:
            filtered_df = df.withColumn("Date1", to_date(df["ExchDateTime"]))\
                        .withColumn("Date1_string", date_format("Date1", "yyyy-MM-dd"))
            filtered_df = filtered_df.filter(filtered_df["Date1_string"] == i)
            
            filtered_df = filtered_df.drop("Date1","Date1_string")
            
            filtered_df.write.format('bigquery').option('table', ('eis-global-dev.BankNiftyTest.BankNifty'))\
            .option("temporaryGcsBucket", "dataproc-cluster-metadata").mode("append").save()
            
            print(f"In bigquery table data is appended for exchange date {i}")