
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DoubleType, TimestampType
from pyspark.sql.types import DecimalType, DateType

from pyspark.sql.functions import split, to_timestamp, concat_ws, to_date, date_format

# from google.cloud import storage
from datetime import datetime

import os
import shutil


# define the Spark Session
spark = SparkSession.builder.appName("test").config("spark.sql.debug.maxToStringFields", 100).getOrCreate()

# Set logging level to "ALL"
spark.sparkContext.setLogLevel("ERROR")


# Create the dataframe of multiple csv files from the directory
df = spark.read.option("header","true").option("inferschema", "True").csv("D:\EIS\BANKNIFTY_WK_EIS_SPOT_2020\BANKNIFTY2FILES\\BANKNIFTY_20200101_Intraday_Preprocessed.csv")


# split the date time column to yearmonthdate compbine and hour minutes seconds combinely so that we can use for storing purpose and filtering date column

df = df.withColumn("YMD", split(df["Date Time"], " ").getItem(0))\
        .withColumn("HH:MM:SS",split(df["Date Time"], " ").getItem(1))

df = df.withColumn("Year",split(df["YMD"], "-").getItem(0))\
        .withColumn("Month",split(df["YMD"], "-").getItem(1))\
        .withColumn("Day",split(df["YMD"], "-").getItem(2))

# Now converting our splitted data into timestamp datatype

df = df.withColumn("DateTime", to_timestamp(concat_ws(" ",concat_ws("-",df["Day"],df["Month"],df["Year"]),df["HH:MM:SS"]),
                                             'dd-MM-yyyy HH:mm:ss'))


#converting ExpiryDate & ExpiryTime to date & time datatype
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

# selecting only necessary columns by renaming with camalcase which are needed for the destination loading as per naming
# convention along with partition columns derived for seperating loaded files


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

df.createOrReplaceTempView("banknifty")

# results = spark.sql("SELECT * FROM banknifty")


# df.select("BidPrice","AskPrice" ).show(10)
# df.printSchema()

unique_dates = ['2020-09-24', '2020-01-14', '2020-09-11', '2020-03-17', '2020-06-12', '2020-06-03', '2020-05-15', '2020-09-07', '2020-04-17', '2020-09-08', '2020-09-04', '2020-11-02', '2020-02-13', '2020-01-07', '2020-10-15', '2020-05-19', '2020-11-25', '2020-04-29', '2020-09-17', '2020-10-23', '2020-08-20', '2020-04-21', '2020-05-27', '2020-01-16', '2020-08-18', '2020-09-29', '2020-07-03', '2020-05-07', '2020-02-03', '2020-11-03', '2020-04-07', '2020-01-31', '2020-02-27', '2020-01-01', '2020-08-07', '2020-05-05', '2020-11-05', '2020-10-22', '2020-02-07', '2020-06-17', '2020-08-06', '2020-08-24', '2020-05-06', '2020-07-27', '2020-11-19', '2020-03-25', '2020-01-02', '2020-10-06', '2020-08-03', '2020-08-28', '2020-11-13', '2020-11-11', '2020-02-18', '2020-07-28', '2020-02-06', '2020-09-28', '2020-07-24', '2020-06-18', '2020-09-09', '2020-10-30', '2020-06-15', '2020-10-19', '2020-04-09', '2020-02-20', '2020-04-08', '2020-08-27', '2020-08-12', '2020-06-24', '2020-10-21', '2020-07-01', '2020-05-13', '2020-03-04', '2020-10-08', '2020-02-26', '2020-09-22', '2020-08-11', '2020-07-10', '2020-03-31', '2020-04-16', '2020-09-02', '2020-09-18', '2020-04-22', '2020-05-14', '2020-11-04', '2020-02-12', '2020-03-06', '2020-10-29', '2020-05-04', '2020-04-15', '2020-08-21', '2020-02-05', '2020-01-09', '2020-06-08', '2020-05-11', '2020-02-25', '2020-10-09', '2020-09-23', '2020-11-12', '2020-03-30', '2020-04-13', '2020-04-03', '2020-08-13', '2020-09-03', '2020-10-07', '2020-11-27', '2020-06-29', '2020-10-16', '2020-03-13', '2020-07-22', '2020-07-20', '2020-09-16', '2020-01-22', '2020-07-16', '2020-08-10', '2020-11-10', '2020-06-02', '2020-04-24', '2020-07-31', '2020-07-14', '2020-11-18', '2020-02-14', '2020-03-02', '2020-10-01', '2020-06-11', '2020-09-21', '2020-07-17', '2020-02-19', '2020-01-23', '2020-03-24', '2020-02-28', '2020-01-10', '2020-07-23', '2020-08-17', '2020-06-23', '2020-08-19', '2020-01-21', '2020-06-09', '2020-01-17', '2020-01-24', '2020-07-21', '2020-07-30', '2020-03-03', '2020-11-20', '2020-04-01', '2020-01-20', '2020-06-10', '2020-04-30', '2020-07-15', '2020-02-11', '2020-01-30', '2020-01-15', '2020-08-31', '2020-01-03', '2020-02-10', '2020-10-13', '2020-07-09', '2020-06-05', '2020-03-27', '2020-01-27', '2020-10-12', '2020-10-27', '2020-07-13', '2020-07-29', '2020-03-26', '2020-08-04', '2020-09-14', '2020-10-14', '2020-08-05', '2020-08-25', '2020-06-22', '2020-10-05', '2020-03-18', '2020-04-27', '2020-07-06', '2020-08-26', '2020-09-01', '2020-03-23', '2020-02-24', '2020-09-10', '2020-06-01', '2020-06-04', '2020-11-09', '2020-05-08', '2020-04-23', '2020-06-25', '2020-01-28', '2020-06-26', '2020-09-25', '2020-06-30', '2020-01-13', '2020-05-12', '2020-11-06', '2020-01-08', '2020-10-20', '2020-06-19', '2020-04-28', '2020-04-20', '2020-02-17', '2020-03-16', '2020-09-15', '2020-09-30', '2020-11-17', '2020-02-04']

# Below logic is used for the writing the dataframe into single csv file with the help of YearMonthDay columns combinely
# for this first fetch the distinct values of date column and create single dataframe without partition and last store the processed
# data into destination folder hirarchy by Year and Month folder 
# and store the data datewise 

# unique_dates = df.select("YMD").distinct().collect()


output_path = "D://EIS//"
output_file_name = "_bankniftyoptions.csv"

# for i in unique_dates:
#     date_str = i["YMD"]
#     output_path_write = f"{output_path}year-{date_str[:4]}//month-{date_str[5:7]}//"
#     output_file_name_write = output_path_write+ date_str

#     # print(output_file_name_write)
#     # print(output_path_write)

#     # filtering the data which contains same date and then stroing it into same month folder
#     df_filtered = df.filter(df["YMD"] == date_str).coalesce(1)
#     df_filtered.write.option("header",True).csv(output_file_name_write)

#     # Stored location contains some other information as well so we defined logic to receusively delete the data 
#     # from the created directory
     
#     list1 = os.listdir(output_file_name_write)

#     for i in list1:
#         if i.endswith(".csv"):
#             print(i, output_file_name_write)
#             os.rename(output_file_name_write +"\\"+ i, output_path_write +"banknifty_"+ date_str +".csv")
#             shutil.rmtree(output_file_name_write, ignore_errors= True)

