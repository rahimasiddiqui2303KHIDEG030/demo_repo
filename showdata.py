
# from getdata import fetch_data,urls
# from pyspark.sql import SparkSession
# # Create a SparkSession

# spark = SparkSession.builder.getOrCreate()
# data = {}
# for a, b in urls.items():
#     data[a] = fetch_data(b)
# df1 = spark.createDataFrame(data["Appointment_API"])
# df1.show(2)
# df2 = spark.createDataFrame(data["Councillor_API"])
# df2.show(2)
# df3 = spark.createDataFrame(data["Patient_councillor_API"])
# df3.show(2)
# df4 = spark.createDataFrame(data["Rating_API"])
# df4.show(2)
# spark.stop()

from getdata import fetch_data, urls
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Fetch data and create DataFrames
dfs = [spark.createDataFrame(fetch_data(url)) for url in urls.values()]

# Show the DataFrames
for df in dfs:
    df.show(2)

# Stop the SparkSession
spark.stop()








