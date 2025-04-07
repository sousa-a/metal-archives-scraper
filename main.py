import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import re
import concurrent.futures
import time

# Set up a requests session with retry logic
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=3)
session.mount("https://", adapter)

CHECKPOINT_FILE = "checkpoint.json"


def fetch_band_page(band):
    band_html = BeautifulSoup(band[0], "html.parser")
    band_name = band_html.text
    band_url = band_html.a["href"]
    country = band[1]
    genre = band[2]
    status = BeautifulSoup(band[3], "html.parser").text

    band_id = re.search(r"/(\d+)", band_url).group(1)

    try:
        band_page_response = session.get(band_url, headers=headers, timeout=5)
        if band_page_response.status_code == 200:
            band_page_soup = BeautifulSoup(band_page_response.content, "html.parser")
            photo_img_tag = band_page_soup.find("a", {"id": "photo"})
            photo_url = photo_img_tag["href"] if photo_img_tag else None
            # os.system("cls" if os.name == "nt" else "clear")
            print(
                f"Band: {band_name}, Country: {country}, Genre: {genre}, Status: {status}, Photo URL: {photo_url}"
            )
            return [band_id, band_name, band_url, country, genre, status, photo_url]
    except requests.RequestException as e:
        print(f"Error fetching band page for {band_name}: {e}")
    return [band_id, band_name, band_url, country, genre, status, None]


def save_to_csv(bands):
    df = pd.DataFrame(
        bands,
        columns=["Band ID", "Name", "URL", "Country", "Genre", "Status", "Photo_URL"],
    )
    df.to_csv(
        "metal_bands.csv",
        mode="a",
        header=not os.path.exists("metal_bands.csv"),
        index=False,
    )


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"letter": None, "start": 0}


def save_checkpoint(letter, start):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"letter": letter, "start": start}, f)


def scrape_letter_bands(letter, existing_bands):
    bands = []
    url_template = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1?sEcho=1&iColumns=4&sColumns=&iDisplayStart={}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false"

    checkpoint = load_checkpoint()
    start = checkpoint["start"] if checkpoint["letter"] == letter else 0
    chunk_size = 1000  # Increased chunk size for fewer I/O operations
    chunk_bands = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            url = url_template.format(letter, start)
            response = session.get(url, headers=headers)
            if response.status_code != 200:
                print(
                    f"Failed to retrieve data for {letter} at offset {start} - Status Code: {response.status_code}"
                )
                break

            data = json.loads(response.content.decode("utf-8"))
            if not data["aaData"]:
                break

            band_data_futures = [
                executor.submit(fetch_band_page, band) for band in data["aaData"]
            ]
            for future in concurrent.futures.as_completed(band_data_futures):
                band_data = future.result()
                if band_data and band_data[2] not in existing_bands:
                    chunk_bands.append(band_data)

                    if len(chunk_bands) >= chunk_size:
                        save_to_csv(chunk_bands)
                        chunk_bands.clear()

            save_checkpoint(letter, start)  # Save progress at each offset
            start += 500
            time.sleep(1)  # Reduced sleep time for faster processing

    if chunk_bands:
        save_to_csv(chunk_bands)


def load_existing_bands():
    if os.path.exists("metal_bands.csv"):
        df = pd.read_csv("metal_bands.csv")
        return set(df["URL"])
    return set()


def main():
    existing_bands = load_existing_bands()
    categories = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["NBR", "~"]

    checkpoint = load_checkpoint()
    start_letter = checkpoint["letter"] if checkpoint["letter"] else categories[0]

    for letter in categories:
        if letter < start_letter:
            continue  # Skip letters that have already been processed
        print(f"Scraping bands in {letter}")
        scrape_letter_bands(letter, existing_bands)
        save_checkpoint(letter, 0)  # Reset start for the next letter

    # Remove checkpoint file after completion
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/",
    }
    main()
