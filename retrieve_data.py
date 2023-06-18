import redis
from json_utils import convert_bytes_to_json
redis_host = "localhost"  # Replace with the actual Redis server host
redis_port = 6379  # Replace with the actual Redis server port
redis_client = redis.Redis(host=redis_host, port=redis_port)

def get_specialization_table(redis_client):
    retrieved_data = redis_client.get("specialization_table")
    retrieved_data_json = convert_bytes_to_json(retrieved_data)
    return retrieved_data_json


def get_avg_rating_table(redis_client):
    retrieved_data = redis_client.get("avg_rating_table")
    retrieved_data_json = convert_bytes_to_json(retrieved_data)
    return retrieved_data_json
    
    
    
# print(get_specialization_table(redis_client))
print(get_avg_rating_table(redis_client))