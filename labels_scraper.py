import os
import re
import time
import json
import requests
import pandas as pd
import concurrent.futures
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup
import re

def fetch_label_data(label):

    def clean_text(text):
        return BeautifulSoup(text, 'html.parser').get_text().replace('\xa0', ' ').strip() if text else ""

    label_name = clean_text(label[1])
    specialization = clean_text(label[2])
    status = clean_text(label[3])
    country = clean_text(label[4])

    label_url_soup = BeautifulSoup(label[1], 'html.parser').a
    label_url = label_url_soup['href'] if label_url_soup and label_url_soup.has_attr('href') else None

    website = None
    if label[5]:
        website_soup = BeautifulSoup(label[5], 'html.parser').find('a')
        website = website_soup['href'] if website_soup and website_soup.has_attr('href') else None

    online_shopping = clean_text(label[6])

    label_id = re.search(r'/(\d+)', label_url).group(1) if label_url else None

    return [label_id, label_name, specialization, status, country, website, online_shopping]

def save_labels_to_csv(labels):

    os.makedirs("labels", exist_ok=True)
    df = pd.DataFrame(labels, columns=["ID", "Name", "Specialization", "Status", "Country", "Website", "Online Shopping"])
    csv_path = os.path.join("labels", "labels.csv")
    
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_path, mode="w", header=True, index=False)

def scrape_labels(letter, existing_labels):
    start_time = time.time()
    labels = []
    url_template = "https://www.metal-archives.com/label/ajax-list/json/1/l/{}?sEcho=1&iColumns=7&sColumns=&iDisplayStart={}&iDisplayLength=200&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&mDataProp_5=5&mDataProp_6=6&iSortCol_0=1&sSortDir_0=asc&iSortingCols=1&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true&bSortable_4=true&bSortable_5=false&bSortable_6=true"
    
    start = 0
    chunk_size = 200

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            url = url_template.format(letter, start)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to retrieve data for {letter} at offset {start} - Status Code: {response.status_code}")
                if response.status_code == 429:
                    print("Rate limited. Waiting 2 seconds before retrying...")
                    time.sleep(2)
                    continue
                break

            data = json.loads(response.content.decode('utf-8'))
            if not data['aaData']:
                break

            label_data_futures = [executor.submit(fetch_label_data, label) for label in data['aaData']]
            for future in concurrent.futures.as_completed(label_data_futures):
                label_data = future.result()
                if label_data and label_data[1] not in existing_labels:  # Avoid duplicates
                    labels.append(label_data)

            start += chunk_size
            print(f"Scraped {len(labels)} labels for letter {letter}")
            time.sleep(1)
            
        elapsed_time = time.time() - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"Time elapsed: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")


    if labels:
        save_labels_to_csv(labels)

def load_existing_labels():
    csv_path = os.path.join("labels", "labels.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        return set(df['ID'])  
    return set()

def main():
    existing_labels = load_existing_labels()
    categories = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["NBR"]

    for letter in categories:
        print(f"Scraping labels for letter {letter}")
        scrape_labels(letter, existing_labels)

if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/"
    }
    main()
