from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import redis
from json_utils import convert_bytes_to_json

def save_specialization_table_to_redis(specialization_table):
    specialization_json = specialization_table.toJSON().collect()

    redis_host = "localhost"  # Replace with the actual Redis server host
    redis_port = 6379  # Replace with the actual Redis server port
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    # Store the data as a JSON array under a single key
    json_array = "[" + ",".join(specialization_json) + "]"
    redis_client.set("specialization_table", json_array)

    # Retrieve the data from Redis
    retrieved_data = redis_client.get("specialization_table")

    redis_client.close()

    # Convert bytes to JSON
    retrieved_data_json = convert_bytes_to_json(retrieved_data)

    return retrieved_data_json

def save_avg_rating_table_to_redis(avg_rating_table):
    avg_rating_json = avg_rating_table.toJSON().collect()

    redis_host = "localhost"  # Replace with the actual Redis server host
    redis_port = 6379  # Replace with the actual Redis server port
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    # Store the data as a JSON array under a single key
    json_array = "[" + ",".join(avg_rating_json) + "]"
    redis_client.set("avg_rating_table", json_array)

    # Retrieve the data from Redis
    retrieved_data = redis_client.get("avg_rating_table")

    redis_client.close()

    # Convert bytes to JSON
    retrieved_data_json = convert_bytes_to_json(retrieved_data)

    return retrieved_data_json

spark = SparkSession.builder.appName("FileReader").getOrCreate()

rating = spark.read.json("rating.json", multiLine=True)
appointment = spark.read.json("appointment.json", multiLine=True)
councillor = spark.read.json("councillor.json", multiLine=True)
patient_councillor = spark.read.json("patient_councillor.json", multiLine=True)

# Perform the joins
join1 = rating.join(appointment, rating.appointment_id == appointment.id, "inner")
join2 = join1.join(councillor, councillor.id == councillor.id, "inner")
join3 = join2.join(patient_councillor, join2.patient_id == patient_councillor.patient_id, "inner")
result_table = join3.select(appointment["patient_id"], councillor["id"].alias("councillor_id"), councillor["specialization"], rating["value"])

specialization_table = result_table.select("specialization", "councillor_id").distinct()
avg_rating_table = result_table.groupBy("councillor_id").avg("value").withColumnRenamed("avg(value)", "average_rating")

# Call the function to save and retrieve data from Redis
retrieved_specialization_data = save_specialization_table_to_redis(specialization_table)
retrieved_avg_rating_data = save_avg_rating_table_to_redis(avg_rating_table)

# Print the retrieved data
print(retrieved_specialization_data)
print(retrieved_avg_rating_data)

specialization_table.show()

avg_rating_table.show()
result_table.show()
