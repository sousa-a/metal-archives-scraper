import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

def extract_discography(soup, band_id):

    """Extract the discography data from the band's page."""
    discography = []
    disco_table = soup.select_one('table.display.discog')  # Locate the table by class
    start_time = time.time()
    if disco_table:
        rows = disco_table.select('tbody tr')  # Select all rows in the tbody
        for row in rows:
            # Extract album name
            name_element = row.select_one('td a')
            name = name_element.text.strip() if name_element else 'Unknown Album'  # Default if not found

            # Extract type, year, and reviews directly from td
            type_element = row.select_one('td:nth-child(2)')
            type_ = type_element.text.strip() if type_element else 'Unknown Type'  # Default if not found

            year_element = row.select_one('td:nth-child(3)')
            year = year_element.text.strip() if year_element else 'Unknown Year'  # Default if not found

            reviews_element = row.select_one('td:nth-child(4) a')
            reviews_text = reviews_element.text.strip() if reviews_element else 'No Reviews'  # Default if not found

            # band_id = row.select_one('td:nth-child(5) a')['href'].split('/')[-1]

            # Append the album data to the discography list
            discography.append([name, type_, year, reviews_text, band_id])

            print(f"Album: {name}, Type: {type_}, Year: {year}, Reviews: {reviews_text}, Band ID: {band_id}")
            print('-' * 40)

    else:
        print("Discography table not found.")

    return discography



def extract_members(soup):
    """Extract the members data from the band's page."""
    members = []
    members_table = soup.select_one('#band_tab_members_current .lineupTable')
    if members_table:
        rows = members_table.select('tbody tr.lineupRow')
        for row in rows:
            name = row.select_one('a').text.strip()
            role = row.find_all('td')[1].text.strip()
            members.append([name, role])
    return members

def scrape_band_page(band_name, band_id):
    """Scrape the band page and save data to CSV files."""
    base_url = f"https://www.metal-archives.com/band/discography/id/{band_id}/tab/all"
    headers = {
        # "User-Agent": "Mozilla/5.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    response = requests.get(base_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve data for {band_name}. Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

     # Check if the soup has the required content
    # if not soup.select_one('#band_tab_discography'):
    #     print(f"No content found for {band_name}. Response Length: {len(response.content)}")
    #     return

    # Extract data from each tab
    discography = extract_discography(soup, band_id)
    # members = extract_members(soup)

    sanitized_band_name = re.sub(r'[<>:"/\\|?*]', '_', band_name)  # Replace invalid characters with an underscore

    # Save discography to CSV
    if discography:
        df_disco = pd.DataFrame(discography, columns=["Album Name", "Type", "Year", "Reviews", "Band ID"])
        # df_disco.to_csv(f"bands_discos/{band_name.replace('/', '_')}_discography.csv", index=False)
        df_disco.to_csv(f"bands_discos/{sanitized_band_name}_discography.csv", index=False)
    # Save members to CSV
    # if members:
    #     df_members = pd.DataFrame(members, columns=["Name", "Role"])
    #     df_members.to_csv(f"{band_name.replace('/', '_')}_members.csv", index=False)

    print(f"Data for {band_name} saved successfully.")

def main():
    # Load band data from CSV
    start_time = time.time()
    bands_df = pd.read_csv("metal_bands.csv")

    print(f"Scraping {len(bands_df)} bands.")

    # Ensure the required columns are present
    if 'Name' not in bands_df.columns or 'URL' not in bands_df.columns not in bands_df.columns:
        print("CSV file is missing 'Band' or 'URL' or 'ID' columns.")
        return

    # Extract band ID from URL and process each band
    for _, row in bands_df.iterrows():
        band_name = row['Name']
        band_url = row['URL']
        # band_id = row['ID']

        # Extract the band ID from the URL
        band_id = band_url.split('/')[-1]  # Get the ID from the last segment of the URL
        os.system('cls' if os.name == 'nt' else 'clear')

        print(f"Scraping {band_name}, ID: {band_id}")
        scrape_band_page(band_name, band_id)
        elapsed_time = time.time() - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"Time elapsed: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")

        time.sleep(1)  # Small delay to avoid server overload

if __name__ == "__main__":
    main()
