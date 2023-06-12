import json

def convert_bytes_to_json(data_bytes):
    data_json = json.loads(data_bytes)
    return data_json
