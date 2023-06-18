# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from getdata import fetch_api_data, urls
# spark= SparkSession.builder.appName("FileReader").getOrCreate()

# def spark_dataframe(spark):
#     spark = SparkSession.builder.appName("FileReader").getOrCreate()
#     rating = spark.createDataFrame(fetch_api_data(urls['rating_api']))
#     appointment = spark.createDataFrame(fetch_api_data(urls['appointment_api']))
#     councillor =spark.createDataFrame(fetch_api_data(urls['councillor_api']))
#     patient_councillor = spark.createDataFrame(fetch_api_data(urls['patient_councillor_api']))
#     return rating, appointment,councillor, patient_councillor
# # Perform the joins
# def perform_joins(rating, appointment, councillor, patient_councillor):
#     join1 = rating.join(appointment, rating.appointment_id == appointment.id, "inner")
#     join2 = join1.join(councillor, councillor.id == councillor.id, "inner")
#     join3 = join2.join(patient_councillor, join2.patient_id == patient_councillor.patient_id, "inner")
#     result_table = join3.select(appointment["patient_id"], councillor["id"].alias("councillor_id"), councillor["specialization"], rating["value"])
#     # specialization_table = result_table.select("specialization", "councillor_id").distinct()
#     # avg_rating_table = result_table.groupBy("councillor_id").avg("value").withColumnRenamed("avg(value)", "average_rating")
#     return result_table
# def calculate_specialization_table(result_table):
#     specialization_table = result_table.select("specialization", "councillor_id").distinct()
#     return specialization_table

# def calculate_avg_rating_table(result_table):
#     avg_rating_table = result_table.groupBy("councillor_id").avg("value").withColumnRenamed("avg(value)", "average_rating")
#     return avg_rating_table


# def run_data_processing(spark):
#     # Create Spark DataFrames
#     rating, appointment, councillor, patient_councillor = spark_dataframe(spark)
#     # Perform Spark joins
#     specialization_table, avg_rating_table, result_table = perform_joins(rating, appointment, councillor, patient_councillor)
#     result_table = perform_joins(rating, appointment, councillor, patient_councillor)
    
#     specialization_table = calculate_specialization_table(result_table)
#     avg_rating_table = calculate_avg_rating_table(result_table)
    
#     specialization_table.show()
#     avg_rating_table.show()
#     result_table.show()
# run_data_processing('spark')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from getdata import fetch_api_data, urls

spark = SparkSession.builder.appName("FileReader").getOrCreate()

def spark_data(spark):
    rating = spark.createDataFrame(fetch_api_data(urls['rating_api']))
    appointment = spark.createDataFrame(fetch_api_data(urls['appointment_api']))
    councillor = spark.createDataFrame(fetch_api_data(urls['councillor_api']))
    patient_councillor = spark.createDataFrame(fetch_api_data(urls['patient_councillor_api']))
    return rating, appointment, councillor, patient_councillor

def perform_joins(rating, appointment, councillor, patient_councillor):
    join1 = rating.join(appointment, rating.appointment_id == appointment.id, "inner")
    join2 = join1.join(councillor, councillor.id == councillor.id, "inner")
    join3 = join2.join(patient_councillor, join2.patient_id == patient_councillor.patient_id, "inner")
    result_table = join3.select(appointment["patient_id"], councillor["id"].alias("councillor_id"), councillor["specialization"], rating["value"])
    return result_table

def calculate_specialization_table(result_table):
    specialization_table = result_table.select("specialization", "councillor_id").distinct()
    return specialization_table

def calculate_avg_rating_table(result_table):
    avg_rating_table = result_table.groupBy("councillor_id").avg("value").withColumnRenamed("avg(value)", "average_rating")
    return avg_rating_table

def run_data_processing(spark):
    rating, appointment, councillor, patient_councillor = spark_data(spark)
    result_table = perform_joins(rating, appointment, councillor, patient_councillor)
    
    specialization_table = calculate_specialization_table(result_table)
    avg_rating_table = calculate_avg_rating_table(result_table)
    
    specialization_table.show()
    avg_rating_table.show()
    result_table.show()

run_data_processing(spark)
