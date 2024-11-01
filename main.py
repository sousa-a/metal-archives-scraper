import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import band_scraper
import re
import concurrent.futures
import time

# TODO 1: Add ID column to the CSV file

def fetch_band_page(band):
    band_name = BeautifulSoup(band[0], 'html.parser').text
    band_url = BeautifulSoup(band[0], 'html.parser').a['href']
    country = band[1]
    genre = band[2]
    status = BeautifulSoup(band[3], 'html.parser').text

    # Retrieve the ID from the url
    band_id = re.search(r'/(\d+)', band_url).group(1)

    # print("*" * 50)
    # print(f"Fetching band page for {band_name}")
    # os.system('cls' if os.name == 'nt' else 'clear')
    # print("*" * 50)

    try:
        retries = 3
        for _ in range(retries):
            band_page_response = requests.get(band_url, headers=headers, timeout=5)
            if band_page_response.status_code == 200:
                time.sleep(1)
                band_page_soup = BeautifulSoup(band_page_response.content, 'html.parser')
                photo_img_tag = band_page_soup.find("a", {"id": "photo"})
                photo_url = photo_img_tag['href'] if photo_img_tag else None

                return [band_id, band_name, band_url, country, genre, status, photo_url]
            elif band_page_response.status_code == 429:
                print("Rate limited. Waiting 2 seconds before retrying...")
                time.sleep(2)
            else:
                break
    except requests.RequestException as e:
        print(f"Error fetching band page for {band_name}: {e}")
    return [band_id, band_name, band_url, country, genre, status, None]

def save_to_csv(bands):
    df = pd.DataFrame(bands, columns=["ID", "Name", "URL", "Country", "Genre", "Status", "Photo_URL"])
    if os.path.exists("metal_bands.csv"):
        df.to_csv("metal_bands.csv", mode="a", header=False, index=False)
    else:
        df.to_csv("metal_bands.csv", mode="w", header=True, index=False)

def scrape_letter_bands(letter, existing_bands):
    bands = []
    url_template = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1?sEcho=1&iColumns=4&sColumns=&iDisplayStart={}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false"

    start = 0
    start_time = time.time()
    chunk_size = 500
    chunk_bands = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            url = url_template.format(letter, start)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to retrieve data for {letter} at offset {start} - Status Code: {response.status_code}")
                if response.status_code == 429:
                    print("Rate limited. Waiting 2 seconds before retrying...")
                    continue
                break

            data = json.loads(response.content.decode('utf-8'))
            if not data['aaData']:
                break

            band_data_futures = [executor.submit(fetch_band_page, band) for band in data['aaData']]
            for future in concurrent.futures.as_completed(band_data_futures):
                band_data = future.result()
                if band_data and band_data[1] not in existing_bands:
                    chunk_bands.append(band_data)

                    if len(chunk_bands) >= chunk_size:
                        # print(f"Saving {chunk_size} bands to CSV")
                        save_to_csv(chunk_bands)
                        
                        os.system('cls' if os.name == 'nt' else 'clear')
                        print("*" * 50)
                        print(f"Saved {len(bands) + len(chunk_bands)} bands in the csv")

                        chunk_bands.clear()

            start += 500
            
            print(f"Letter {letter}")
            print("*" * 50)

            # print(f"Elapsed time: {time.time() - start_time:.2f} seconds ")
            time.sleep(20)

    if chunk_bands:
        save_to_csv(chunk_bands)

def load_existing_bands():
    if os.path.exists("metal_bands.csv"):
        df = pd.read_csv("metal_bands.csv")
        return set(df['URL'])
    return set()

def main():
    existing_bands = load_existing_bands()
    categories = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["NBR", "~"]

    for letter in categories:
        print(f"Scraping bands in {letter}")
        scrape_letter_bands(letter, existing_bands)

    band_scraper.main()

if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/"
    }
    main()