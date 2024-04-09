import requests

# Define the URL endpoint
url = "https://data.moa.gov.tw/Service/OpenData/FromM/AnimalTransData.aspx/api/v1/PorkTransType"

# Define the query parameters
params = {
    "TransDate": "1071004",
    "MarketName": "花蓮縣"
}

# Make the GET request
response = requests.get(url, params=params)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Print the response content (data)
    print(response.json())
else:
    # Print an error message if the request failed
    print(f"Error: {response.status_code}")

