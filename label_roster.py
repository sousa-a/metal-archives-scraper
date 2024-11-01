import requests
import pandas as pd
import time
import os
import glob
from bs4 import BeautifulSoup

def get_last_processed_label():
    output_file_path = "labels_rosters/combined_roster.csv"
    if not os.path.exists(output_file_path):
        return None
    
    processed_df = pd.read_csv(output_file_path)
    if processed_df.empty:
        return None
    
    last_label_id = processed_df['Label ID'].iloc[-1]
    return last_label_id

def fetch_band_data(label_id):
    url = f"https://www.metal-archives.com/label/ajax-bands/nbrPerPage/100/id/{label_id}?sEcho=1&iColumns=3&sColumns=&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json() 
    else:
        print(f"Failed to retrieve data for label ID {label_id}. Status Code: {response.status_code}")
        return None

def append_to_csv(file_path, records):
    file_exists = os.path.isfile(file_path)
    df = pd.DataFrame(records, columns=['Label ID', 'Band ID'])
    df.to_csv(file_path, mode='a', header=not file_exists, index=False)

def process_band_data(label_id, file_path):
    data = fetch_band_data(label_id)
    
    if data and 'aaData' in data:
        bands = data['aaData']
        
        band_records = []
        for band in bands:
            band_link = band[0]
            
            soup = BeautifulSoup(band_link, 'html.parser')
            band_a_tag = soup.find('a')
            if band_a_tag and 'href' in band_a_tag.attrs:
                band_url = band_a_tag['href'] 
                band_id = band_url.split('/')[-1] 
                band_records.append({"Label ID": label_id, "Band ID": band_id})
        
        append_to_csv(file_path, band_records)

def main():
    labels_df = pd.read_csv("labels/labels.csv")

    if 'Name' not in labels_df.columns or 'Label ID' not in labels_df.columns:
        print("CSV file is missing 'Name' or 'Label ID' columns.")
        return

    last_processed_label = get_last_processed_label()
    start_processing = False if last_processed_label else True

    output_file_path = "labels_rosters/combined_roster.csv"
    if not os.path.exists(output_file_path):
        open(output_file_path, 'w').close()

    for _, row in labels_df.iterrows():
        label_name = row['Name']
        label_id = row['Label ID']

        if not start_processing:
            if label_id == last_processed_label:
                start_processing = True
            continue

        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"Processing {label_name}, ID: {label_id}")
        process_band_data(label_id, output_file_path)

        time.sleep(1)

    print("All band records have been processed and saved successfully.")

if __name__ == "__main__":
    main()
