
import re
import logging
import time
import random
import requests
import yaml
import os
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
from lxml import etree

from misc_helpers import load_random_headers, init_logging, previously_scraped, arbeidsplassen_xpaths
from process_data import process_data, store_data


def scrape_single_ad(url: str,
                     xpaths: dict,
                     headers: dict,
                     uuid: str) -> dict:
    """Extract the data from a single ad.

    Function scrapes the HTML elements.

    Args:
        url: Url where the ad is.
        xpaths: xpaths for the given type of ad.
        headers: Request headers.

    Returns:
        A dictionary containing all scraped data as HTML elements.
        This dict represents an row in the CSV result file.
    """
    try:
        r = requests.get(url, headers=headers)
    except requests.exceptions.ConnectionError as e:
        logging.error(f'ConnectionError for {e}')
        return

    if r.status_code != 200:
        logging.error(f'SCRAPE PAGE RESPONSE CODE {r.status_code}, URL: {url}')
        return

    tree = etree.HTML(r.text)

    result_dict = {
        'url': url,
        'scrape_time': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
        'uuid': uuid
    }

    for k, v in xpaths.items():
        content = tree.xpath(v)

        if not content:
            result_dict[k] = None
        else:
            result_dict[k] = etree.tostring(content[0],
                                            method='html',
                                            encoding='unicode')

    if result_dict['title']:
        title = etree.tostring(etree.fromstring(result_dict['title']),
                               method='text',
                               encoding='unicode')
        logging.info(f'TITLE: {title.rstrip()}')

    return result_dict


def scrape_ads_list(ad_urls: list,
                    scraped_codes: list,
                    headers: dict,
                    xpaths: dict,
                    scrape_folder: str,
                    curr_time: str,
                    store_data_bool=True) -> None:
    """Scrape a list of ads and store the data
    
    Args:
        ad_urls: List of AD urls to scrape.
        scraped_codes: Regex pattern for finding id in url.
        headers: Request headers.
        xpaths: XPATHS used to grab the correct data.
        scrape_folder: Where to store the scrape data.
        curr_time: Start time of scrape, used for filename.
        toggle: Current button toggle for search.
        store_data_bool: Bool deciding to save the data or not.
    """
    uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}')
    page_ads = []

    for url in ad_urls:
        uuid = uuid_pattern.search(url).group(0)

        # Check if ad has been scraped before.
        if uuid in scraped_codes:
            logging.info(f'ALREADY SCRAPED: {url}')
            continue

        scraped_codes.append(uuid)
        result = scrape_single_ad(url=url,
                                  headers=headers,
                                  xpaths=xpaths,
                                  uuid=uuid)

        if not result:
            continue

        page_ads.append(result)
        time.sleep(random.uniform(0.75, 1.5))

    if store_data_bool:
        processed_ads = process_data(page_ads)
        store_data(processed_ads, scrape_folder, curr_time)


def iterate_pages(curr_time: str,
                  scrape_folder: str,
                  headers: dict,
                  scraped_codes: list,
                  base_url: str,
                  toggle: str) -> None:
    """Iterate one page filled with ads.

    Iterate pages goes through all pages and extract ads
    to scrape. If no ads are found on the page, the function
    returns. The function stores all data in batches at the
    end of the page scrape.

    Args:
        curr_time: Start time of scrape, used for filename.
        folder: Where to store the scrape data.
        headers: Request headers.
        scraped_codes: Regex pattern for finding id in url.
        base_url: Base URL used repetitively.
        toggle: Current button toggle for search.
    """
    xpaths = arbeidsplassen_xpaths()
    ad_pattern = re.compile(r'(\/stillinger\/stilling\/.+)')

    for page_number in range(100):
        logging.info(f'SCRAPING PAGE {page_number + 1}')

        time.sleep(random.uniform(1.5, 2.5))

        if page_number == 0:
            url = f'{base_url}/stillinger?size=100&{toggle}&v=2'
        else:
            url = f'{base_url}/stillinger?from={page_number * 100}&size=100&{toggle}&v=2'

        r = requests.get(url, headers=headers)

        if r.status_code == 400:
            return
        elif r.status_code != 200:
            logging.error(f'ITERATE PAGE RESPONSE CODE {r.status_code}, URL: {url}')
            continue

        headers['Referer'] = url
        soup = BeautifulSoup(r.text, 'html.parser')

        a_tags = soup.find_all('a')

        all_urls = [u for u in a_tags if isinstance(u, str)]
        all_urls = [u.get('href') for u in a_tags]

        ad_urls = [base_url + u for u in all_urls if ad_pattern.search(u)]

        if not ad_urls:
            logging.info('NO ADS ON PAGE')
            return
        
        # All AD ad for the page is collected, call function to scrape the data
        # for each ad.
        scrape_ads_list(ad_urls,
                        scraped_codes,
                        headers,
                        xpaths,
                        scrape_folder,
                        curr_time)
    

def get_toggles(full_scrape: bool,
                base_url: str,
                headers: dict) -> list:
    """Get the different toggles available.
    
    When scraping arbeidsplassen, page 100 is the last
    available page, yielding a maximum of 100*100 ads.
    To ensure all data is scarped, different toggles are
    applied, such as filtering by langauge, location, field,
    etc.

    Args:
        full_scrape: If True apply all toggles, if False scrape only daily ads.
        base_url: Arbeidsplassen base url.
        headers: Request headers.
        
    Returns:
        A list of all toggles on the website.
    """
    if not full_scrape:
        return ['&published=now%2Fd']
    else:
        url = f'{base_url}/stillinger'

        try:
            r = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError as e:
            logging.error(f'ConnectionError for {e}')
            return

        soup = BeautifulSoup(r.text, 'html.parser')
        inputs = soup.find_all('input')
        inputs = [input_tag for input_tag in inputs
                  if 'checkbox' or 'radio' in input_tag.get('id', '')]

        toggles = [f'{input.get("name")}={input.get("value")}'
                   for input in inputs]

        if 'q=' in toggles:
            toggles.remove('q=')
        
        return toggles


def main():
    with open('parameters.yml', 'r') as file:
        flags = yaml.safe_load(file)

    curr_time = datetime.today().strftime('%Y_%m_%d')
    init_logging(f'{flags["log_folder"]}/arbeidsplassen_{curr_time}.log')

    headers = load_random_headers()
    base_url = 'https://arbeidsplassen.nav.no'

    toggles = get_toggles(flags['full_scrape'], base_url, headers)

    for t in toggles:
        logging.info(f'SCRAPING ARBEIDSPLASSEN WITH {t}')

        if flags['ignore_previously_scraped']:
            scraped_urls = previously_scraped(scrape_folder=flags['scrape_folder'], n_files=50)
        else:
            scraped_urls = []

        iterate_pages(curr_time=curr_time,
                      scrape_folder=flags['scrape_folder'],
                      headers=headers,
                      scraped_codes=scraped_urls,
                      base_url=base_url,
                      toggle=t)

        logging.info(f'FINISHED SCRAPING ARBEIDSPLASSEN WITH {t}')
    

if __name__ == "__main__":
    main()
