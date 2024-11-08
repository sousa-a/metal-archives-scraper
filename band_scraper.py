import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

MASTER_DISCO_FILE = 'bands_discos/all_bands_discography.csv'

def get_last_processed_band_id():
    if not os.path.exists(MASTER_DISCO_FILE):
        return None
    df = pd.read_csv(MASTER_DISCO_FILE)
    if df.empty:
        return None
    return str(df['Band ID'].iloc[-1]) 

def extract_discography(soup, band_id):
    discography = []
    disco_table = soup.select_one('table.display.discog')
    if disco_table:
        rows = disco_table.select('tbody tr')
        for row in rows:
            name_element = row.select_one('td a')
            name = name_element.text.strip() if name_element else 'Unknown Album'

            type_element = row.select_one('td:nth-child(2)')
            type_ = type_element.text.strip() if type_element else 'Unknown Type'

            year_element = row.select_one('td:nth-child(3)')
            year = year_element.text.strip() if year_element else 'Unknown Year'

            reviews_element = row.select_one('td:nth-child(4) a')
            reviews_text = reviews_element.text.strip() if reviews_element else 'No Reviews'

            discography.append([name, type_, year, reviews_text, band_id])
    else:
        print(f"No discography table found for Band ID: {band_id}.")
    return discography

def scrape_band_page(band_name, band_id, retries=3):
    base_url = f"https://www.metal-archives.com/band/discography/id/{band_id}/tab/all"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }
    for attempt in range(retries):
        response = requests.get(base_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            return extract_discography(soup, band_id)
        else:
            print(f"Attempt {attempt + 1} failed for Band ID {band_id}. Retrying...")
            time.sleep(1)
    print(f"Failed to retrieve data for {band_name} after {retries} attempts.")
    return []

def save_to_master_file(discographies):
    columns = ["Album Name", "Type", "Year", "Reviews", "Band ID"]
    df_disco = pd.DataFrame(discographies, columns=columns)
    if os.path.exists(MASTER_DISCO_FILE):
        df_disco.to_csv(MASTER_DISCO_FILE, mode='a', header=False, index=False)
    else:
        df_disco.to_csv(MASTER_DISCO_FILE, mode='w', header=True, index=False)
    print(f"Batch of data appended to {MASTER_DISCO_FILE} successfully.")

def main():
    start_time = time.time()
    bands_df = pd.read_csv("metal_bands.csv", low_memory=False)

    if 'Name' not in bands_df.columns or 'URL' not in bands_df.columns:
        print("CSV file is missing 'Name' or 'URL' columns.")
        return

    last_processed_band_id = get_last_processed_band_id()
    start_processing = False if last_processed_band_id else True

    discographies = []
    batch_size = 500

    for _, row in bands_df.iterrows():

        band_name = row['Name']
        band_url = row['URL']
        band_id = band_url.split('/')[-1]

        if not start_processing:
            if band_id == last_processed_band_id:
                start_processing = True
            continue

        print(f"Scraping {band_name}, ID: {band_id}")
        band_discography = scrape_band_page(band_name, band_id)
        if band_discography:
            discographies.extend(band_discography)

        if len(discographies) >= batch_size:
            save_to_master_file(discographies)
            discographies = []

        elapsed_time = time.time() - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"Time elapsed: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
        os.system('cls' if os.name == 'nt' else 'clear')


    if discographies:
        save_to_master_file(discographies)

if __name__ == "__main__":
    main()
