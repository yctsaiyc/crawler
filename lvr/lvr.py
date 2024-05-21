from datetime import date
import requests
import zipfile
import os


today = date.today()
if today.month in [1, 2, 3]:
    SEASON = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    SEASON = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    SEASON = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    SEASON = f"{today.year - 1911}S3"


url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={SEASON}&type=zip&fileName=lvr_landcsv.zip"
print("url:", url)

response = requests.get(url)

if response.status_code == 200:
    zip_filename = f"lvr_landcsv_{SEASON}.zip"

    # Write the content of the response to the file
    with open(zip_filename, "wb") as file:
        file.write(response.content)
    print(f"File downloaded and saved as {zip_filename}")

    # Unzip the file
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        # Extract all the contents
        zip_ref.extractall(zip_filename.rstrip('.zip'))
    print(f"File unzipped successfully")

    os.remove(zip_filename)
    print(f"Deleted {zip_filename}")

else:
    print("Failed to download the file. Status code:", response.status_code)
