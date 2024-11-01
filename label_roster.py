import requests
import pandas as pd
import time
import os
import glob
from bs4 import BeautifulSoup  # Import BeautifulSoup for parsing HTML

def get_last_processed_label():
    processed_files = glob.glob('labels_rosters/*_roster.csv')
    if not processed_files:
        return None
    
    last_file = max(processed_files, key=os.path.getctime)
    last_label = os.path.basename(last_file).replace('_roster.csv', '') 
    return last_label

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
        return response.json()  # Return JSON response
    else:
        print(f"Failed to retrieve data for label ID {label_id}. Status Code: {response.status_code}")
        return None

def append_to_csv(file_path, records):
    # Check if the file already exists to determine if we need to write the header
    file_exists = os.path.isfile(file_path)
    
    # Create a DataFrame and append to CSV
    df = pd.DataFrame(records, columns=['Label ID', 'Band ID'])
    df.to_csv(file_path, mode='a', header=not file_exists, index=False)

def process_band_data(label_id, file_path):
    data = fetch_band_data(label_id)
    
    if data and 'aaData' in data:
        bands = data['aaData']  # Get the list of bands
        
        # Prepare records
        band_records = []
        for band in bands:
            band_link = band[0]  # First element is the band name (with link)
            
            # Use BeautifulSoup to extract the href from the anchor tag
            soup = BeautifulSoup(band_link, 'html.parser')
            band_a_tag = soup.find('a')
            if band_a_tag and 'href' in band_a_tag.attrs:
                band_url = band_a_tag['href']  # Extract the URL
                band_id = band_url.split('/')[-1]  # Extract the band ID from the URL
                band_records.append({"Label ID": label_id, "Band ID": band_id})
        
        # Append records to CSV
        append_to_csv(file_path, band_records)

def main():
    labels_df = pd.read_csv("labels/labels.csv")

    if 'Name' not in labels_df.columns or 'ID' not in labels_df.columns:
        print("CSV file is missing 'Name' or 'ID' columns.")
        return

    last_processed_label = get_last_processed_label()
    start_processing = False if last_processed_label else True

    output_file_path = "labels_rosters/combined_roster.csv"

    # Clear the output file if it already exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    for _, row in labels_df.iterrows():
        label_name = row['Name']
        label_id = row['ID']

        if not start_processing:
            if label_name == last_processed_label:
                start_processing = True
            continue

        print(f"Processing {label_name}, ID: {label_id}")
        process_band_data(label_id, output_file_path)

        time.sleep(1)

    print("All band records have been processed and saved successfully.")

if __name__ == "__main__":
    main()
