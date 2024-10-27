import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import band_scraper
import re
import concurrent.futures
import time

# def download_image(url, band_name):
#     if not url:
#         return None

#     sanitized_band_name = re.sub(r'[<>:"/\\|?*]', '_', band_name)
#     os.makedirs("band_images", exist_ok=True)

#     file_name = url.split('/')[-1].split('?')[0]
#     file_path = f"band_images/{sanitized_band_name}_{file_name}"

#     if not os.path.exists(file_path):
#         try:
#             response = requests.get(url, timeout=5)
#             if response.status_code == 200:
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#             else:
#                 print(f"Failed to download image for {band_name} - Status Code: {response.status_code}")
#                 return None
#         except requests.RequestException as e:
#             print(f"Error downloading image for {band_name}: {e}")
#             return None
#     return file_path

def fetch_band_page(band):
    band_name = BeautifulSoup(band[0], 'html.parser').text
    band_url = BeautifulSoup(band[0], 'html.parser').a['href']
    country = band[1]
    genre = band[2]
    status = BeautifulSoup(band[3], 'html.parser').text

    print(f"Fetching band page for {band_name}")

    try:
        retries = 3
        for _ in range(retries):
            band_page_response = requests.get(band_url, headers=headers, timeout=5)
            if band_page_response.status_code == 200:
                time.sleep(1)
                band_page_soup = BeautifulSoup(band_page_response.content, 'html.parser')
                logo_img_tag = band_page_soup.find("a", {"id": "logo"})
                photo_img_tag = band_page_soup.find("a", {"id": "photo"})

                # logo_url = logo_img_tag['href'] if logo_img_tag else None
                photo_url = photo_img_tag['href'] if photo_img_tag else None
                # image_url = logo_url if logo_url else photo_url

                return [band_name, band_url, country, genre, status, photo_url]
            elif band_page_response.status_code == 429:
                print("Rate limited. Waiting 2 seconds before retrying...")
                time.sleep(2)
            else:
                break
    except requests.RequestException as e:
        print(f"Error fetching band page for {band_name}: {e}")
    return [band_name, band_url, country, genre, status, None]

def scrape_letter_bands(letter):
    bands = []
    url_template = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1?sEcho=1&iColumns=4&sColumns=&iDisplayStart={}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false"

    start = 0
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            url = url_template.format(letter, start)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to retrieve data for {letter} at offset {start} - Status Code: {response.status_code}")
                if response.status_code == 429:
                    print("Rate limited. Waiting 2 seconds before retrying...")
                    continue  # Retry this iteration after waiting
                break

            data = json.loads(response.content.decode('utf-8'))
            if not data['aaData']:
                break

            band_data_futures = [executor.submit(fetch_band_page, band) for band in data['aaData']]
            for future in concurrent.futures.as_completed(band_data_futures):
                band_data = future.result()
                if band_data:  # Only add non-None results
                    bands.append(band_data)
            start += 500
            print(f"Scraped {len(bands)} bands so far")
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
            time.sleep(20)  # Add a slight delay between pages

    return bands

def main():
    all_bands = []
    # categories = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["NBR", "~"]
    categories = ["~"]
    for letter in categories:
        print(f"Scraping bands in {letter}")
        bands = scrape_letter_bands(letter)
        all_bands.extend(bands)

    all_bands = [band for band in all_bands if band is not None]
    all_bands_with_id = [[i + 1] + band for i, band in enumerate(all_bands)]

    df = pd.DataFrame(all_bands_with_id, columns=["ID", "Band", "URL", "Country", "Genre", "Status", "Image Url"])
    df.to_csv("metal_bands.csv", index=False)
    print("Data saved to metal_bands.csv")

    print("Running Band Page Scraper")
    band_scraper.main()

if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/"
    }
    main()
