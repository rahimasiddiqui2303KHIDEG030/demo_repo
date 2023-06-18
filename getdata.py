import requests
    #first we import request
def fetch_api_data(urls):
    # Fetches data from multiple API URLs and returns the JSON response
    response = requests.get(urls)
    status_code = response.status_code
    if status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data. Status code: {status_code}")
    
    return None
# Define the API URLs with names
urls={
            "appointment_api":"https://xloop-dummy.herokuapp.com/appointment",
            "councillor_api":"https://xloop-dummy.herokuapp.com/councillor",
            "patient_councillor_api":"https://xloop-dummy.herokuapp.com/patient_councillor",
            "rating_api":"https://xloop-dummy.herokuapp.com/rating" 
}






