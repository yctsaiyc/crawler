import requests
import pandas as pd

# Send a GET request to the API
url = "https://www.cosmed.com.tw/api/getStore.aspx?t=store&c=&d=&s="
response = requests.get(url)

# Parse the JSON response
data = response.json()

# Initialize lists to store data
store_names = []
zip_code_names_1 = []
zip_code_names_2 = []
addresses = []

# Extract data from each store entry
for store in data["data"]:
    store_names.append(store["StoreNM"])
    zip_code_names_1.append(store["ZipCodeName1"])
    zip_code_names_2.append(store["ZipCodeName2"])
    addresses.append(store["Address"])

# Concatenate zip code names and address for column 2
addresses = [f"{z1}{z2}{addr.split('(')[0]}" for z1, z2, addr in zip(zip_code_names_1, zip_code_names_2, addresses)]

# Create a DataFrame
df = pd.DataFrame({
    "Store Name": store_names,
    "City": zip_code_names_1,
    "District": zip_code_names_2,
    "Address": addresses
})

# Save the DataFrame to a CSV file
df.to_csv("cosmed.csv", index=False)
