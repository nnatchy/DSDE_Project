import requests
import zipfile
import os

def download_and_extract(year, base_path="/opt/airflow/data"):
    output_dir =  os.path.join(base_path, "scopus")
    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, f"{year}.zip")
    extract_path = os.path.join(output_dir, str(year))

    # Check if the directory already exists
    if os.path.exists(extract_path):
        print(f"Directory for {year} already exists. Skipping download and extraction.")
        return

    # Check if the zip file already exists
    if not os.path.exists(zip_path):
        url = f"https://github.com/nnatchy/DSDE_Project/raw/main/{year}_test.zip"
        response = requests.get(url)
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {year}.zip")
        else:
            print(f"Failed to download {url}")
            return

    # Extract the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
        print(f"Extracted {year}.zip to {extract_path}")

    # Remove the zip file after extraction
    os.remove(zip_path)
    print(f"Removed {zip_path}")

def download_files():
    years = range(2018, 2024)
    for year in years:
        download_and_extract(year)
