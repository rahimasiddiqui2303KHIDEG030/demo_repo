# import redis
# from json_utils import convert_bytes_to_json

# def save_specialization_table_to_redis(specialization_table):
#     specialization_json = specialization_table.toJSON().collect()
#     redis_host = "localhost"  # Replace with the actual Redis server host
#     redis_port = 6379  # Replace with the actual Redis server port
#     redis_client = redis.Redis(host=redis_host, port=redis_port)
#     # Store the data as a JSON array under a single key
#     json_array = "[" + ",".join(specialization_json) + "]"
#     redis_client.set("specialization_table", json_array)
#     # Retrieve the data from Redis
#     retrieved_data = redis_client.get("specialization_table")
#     redis_client.close()
#     # Convert bytes to JSON
#     retrieved_data_json = convert_bytes_to_json(retrieved_data)
#     return retrieved_data_json
# def save_avg_rating_table_to_redis():
#     avg_rating_json = avg_rating_table.toJSON().collect()
#     redis_host = "localhost"  # Replace with the actual Redis server host
#     redis_port = 6379  # Replace with the actual Redis server port
#     redis_client = redis.Redis(host=redis_host, port=redis_port)
#     # Store the data as a JSON array under a single key
#     json_array = "[" + ",".join(avg_rating_json) + "]"
#     redis_client.set("avg_rating_table", json_array)
#     # Retrieve the data from Redis
#     retrieved_data = redis_client.get("avg_rating_table")
#     redis_client.close()
#     # Convert bytes to JSON
#     retrieved_data_json = convert_bytes_to_json(retrieved_data)
#     return retrieved_data_json
# print(save_avg_rating_table_to_redis(avg_rating_table))
# # def load_data():
# #     # Call the function to save and retrieve data from Redis
# #     retrieved_specialization_data = save_specialization_table_to_redis(specialization_table)
# #     retrieved_avg_rating_data = save_avg_rating_table_to_redis(avg_rating_table)
# #     # Print the retrieved data
# #     print(retrieved_specialization_data)
# #     print(retrieved_avg_rating_data)
# # load_data()
import redis
from json_utils import convert_bytes_to_json
from pyspark.sql import SparkSession
from transform import calculate_avg_rating_table
from transform import perform_joins
from transform import calculate_specialization_table, spark_data

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
rating, appointment, councillor, patient_councillor = spark_data(spark)
result_table = perform_joins(rating, appointment, councillor, patient_councillor)
specialization_table = calculate_specialization_table(result_table)
avg_rating_table = calculate_avg_rating_table(result_table)

specialization_json = save_specialization_table_to_redis(specialization_table)
avg_rating_json = save_avg_rating_table_to_redis(avg_rating_table)

print(specialization_json)
print(avg_rating_json)
