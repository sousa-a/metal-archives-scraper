import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import band_scraper
import re
import time
import winsound

def download_image(url, band_name):
    # Sanitize the band name for a valid filename
    sanitized_band_name = re.sub(r'[<>:"/\\|?*]', '_', band_name)
    # Create a directory for images if it doesn't exist
    os.makedirs("band_images", exist_ok=True)

    # Get the image file name from the URL
    file_name = url.split('/')[-1].split('?')[0]  # Get the last part of the URL before any query parameters
    file_path = f"band_images/{sanitized_band_name}_{file_name}"  # Path to save the image

    # Download the image if it doesn't already exist
    if not os.path.exists(file_path):
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                f.write(response.content)
            # print(f"Downloaded image for {band_name}: {file_path}")
        else:
            print(f"Failed to download image for {band_name} - Status Code: {response.status_code}")
            file_path = None  # Return None if download failed

    return file_path

def scrape_letter_bands(letter):
    bands = []
    url_template = "https://www.metal-archives.com/browse/ajax-letter/l/{}/json/1?sEcho=1&iColumns=4&sColumns=&iDisplayStart={}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false"

    # Define headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json",
        "Referer": "https://www.metal-archives.com/"
    }

    start = 0  # Offset for pagination

    while True:
        start_time = time.time()
        # Create URL with the current start position for pagination
        url = url_template.format(letter, start)
        response = requests.get(url, headers=headers)

        # Ensure the response is valid
        if response.status_code != 200:
            print(f"Failed to retrieve data for {letter} at offset {start} - Status Code: {response.status_code}")
            break

        # Parse JSON from the content
        data = json.loads(response.content.decode('utf-8'))

        # Check if there's any data on this page
        if not data['aaData']:
            break

        # Extract band details from each row in 'aaData'
        for band in data['aaData']:
            band_name = BeautifulSoup(band[0], 'html.parser').text
            band_url = BeautifulSoup(band[0], 'html.parser').a['href']
            country = band[1]
            genre = band[2]
            status = BeautifulSoup(band[3], 'html.parser').text

            # winsound.Beep(1000, 100)

            # print(f"Scraping {band_name}")
            
            # Now, scrape the band page to get image URLs
            band_page_response = requests.get(band_url, headers=headers)
            if band_page_response.status_code == 200:
                band_page_soup = BeautifulSoup(band_page_response.content, 'html.parser')
                
                # Find logo and photo images
                logo_img_tag = band_page_soup.find("a", {"id": "logo"})
                photo_img_tag = band_page_soup.find("a", {"id": "photo"})
                
                logo_url = logo_img_tag['href'] if logo_img_tag else None
                photo_url = photo_img_tag['href'] if photo_img_tag else None
                
                # Download the logo image and get the local path
                # logo_path = download_image(logo_url, band_name) if logo_url else None
                # Download the photo image and get the local path
                # photo_path = download_image(photo_url, band_name) if photo_url else None
                
                # Choose the logo path as the main image for simplicity
                # image_file_path = logo_path if logo_path else photo_path
                image_url = logo_url if logo_url else photo_url

            else:
                print(f"Failed to retrieve band page for {band_name} - Status Code: {band_page_response.status_code}")
                image_url = None  # No image path if band page fails

            # Add band info to the list with the image path
            bands.append([band_name, band_url, country, genre, status, image_url])

        # Move to the next 500 records
        start += 500

        print (f"Scraped {len(bands)} bands so far")
        print(f"Time taken: {time.time() - start_time:.2f} seconds")
        #Beep
        winsound.Beep(1000, 100)
        print('-' * 40)

    
    return bands

def main():
    all_bands = []
    # Define the categories, including each letter, 'NBR', and '~'
    categories = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["NBR", "~"]
    
    # Iterate through each category
    for letter in categories:
        print(f"Scraping bands in {letter}")
        bands = scrape_letter_bands(letter)
        all_bands.extend(bands)

    all_bands_with_id = [[i + 1] + band for i, band in enumerate(all_bands)]

    # Save the data to a CSV file
    df = pd.DataFrame(all_bands_with_id, columns=["ID", "Band", "URL", "Country", "Genre", "Status", "Image Url"])
    df.to_csv("metal_bands.csv", index=False)
    print("Data saved to metal_bands.csv")

    # Play a sound to indicate completion
    winsound.Beep(1200, 1000)
    winsound.Beep(1200, 1000)
    winsound.Beep(1200, 1000)

    # Run the band_scraper.py script
    print("Running Band Page Scraper")
    print("This may take a while...")
    print("Check the bands_discos folder for the results.")
    print("------------------------------------------------")
    band_scraper.main()

if __name__ == "__main__":
    main()
