import requests
def fetch_data(urls):
    response = requests.get(urls)
    status_code = response.status_code
    if status_code == 200:
        return response.json()
    else:
        return None
# Define the API URLs with names
urls={
            "Appointment_API":"https://xloop-dummy.herokuapp.com/appointment",
            "Councillor_API":"https://xloop-dummy.herokuapp.com/councillor",
            "Patient_councillor_API":"https://xloop-dummy.herokuapp.com/patient_councillor",
            "Rating_API":"https://xloop-dummy.herokuapp.com/rating" 
}





# # getdata.py
# import requests
# def fetch_data_from_apis(api_url):
#     response = requests.get(api_url)
#     status_code = response.status_code
#     if status_code == 200:
#         return response.json()
#     else:
#         return None
# Define the API URLs with names
# api_urls = {
#     "Councillor_API": "https://xloop-dummy.herokuapp.com/councillor",
#     "Patient_API": "https://xloop-dummy.herokuapp.com/patient",
#     "Patient_Councillor_API": "https://xloop-dummy.herokuapp.com/patient_councillor",
#     "Rating_API": "https://xloop-dummy.herokuapp.com/rating"
# }