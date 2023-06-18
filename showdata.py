from getdata import fetch_api_data, urls
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Fetch data and create DataFrames
dfs = [spark.createDataFrame(fetch_api_data(url)) for url in urls.values()]

# Show the DataFrames
for df in dfs:
    df.show(2)

# Stop the SparkSession
spark.stop()








