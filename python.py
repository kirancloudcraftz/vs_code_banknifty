# used this script in local machine for testing purpose

from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType

from pyspark.sql.functions import split, to_timestamp, concat_ws, to_date


import os
import shutil


# define the Spark Session
spark = SparkSession.builder.appName("test").getOrCreate()

# Set logging level to "ALL"
spark.sparkContext.setLogLevel("ERROR")


# Create the dataframe of multiple csv files from the directory
df = spark.read.option("header","true").option("inferschema", "True").csv("D:\EIS\BANKNIFTY_WK_EIS_SPOT_2020\BANKNIFTY2FILES\\")


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

df.printSchema()


# Below logic is used for the writing the dataframe into single csv file with the help of YearMonthDay columns combinely
# for this first fetch the distinct values of date column and create single dataframe without partition and last store the processed
# data into destination folder hirarchy by Year and Month folder 
# and store the data datewise 

unique_dates = df.select("YMD").distinct().collect()

output_path = "D://EIS//"
output_file_name = "_bankniftyoptions.csv"

for i in unique_dates:
    date_str = i["YMD"]
    output_path_write = f"{output_path}year-{date_str[:4]}//month-{date_str[5:7]}//"
    output_file_name_write = output_path_write + date_str

#     print(output_file_name_write)
#     print(output_path_write)

    # filtering the data which contains same date and then stroing it into same month folder
    df_filtered = df.filter(df["YMD"] == date_str).coalesce(1)
    df_filtered.write.option("header",True).csv(output_file_name_write)

    # Stored location contains some other information as well so we defined logic to receusively delete the data 
    # from the created directory
     
    list1 = os.listdir(output_file_name_write)

    for i in list1:
        if i.endswith(".csv"):
            print(i, output_file_name_write)
            os.rename(output_file_name_write +"\\"+ i, output_path_write +"banknifty_"+ date_str +".csv")
            shutil.rmtree(output_file_name_write, ignore_errors= True)

