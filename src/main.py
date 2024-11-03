import re
import logging
import time
import random
import yaml

from collections.abc import Generator
from datetime import datetime
from bs4 import BeautifulSoup
from lxml import etree

from misc_helpers import load_random_headers, init_logging, previously_scraped, arbeidsplassen_xpaths
from requests_wrapper import requests_wrapper
from process_data import process_data, store_data


def scrape_single_ad(url: str,
                     xpaths: dict,
                     headers: dict,
                     uuid: str,
                     store_html: bool) -> dict | None:
    """Extract the data from a single ad.

    Function scrapes the HTML elements.

    Args:
        url: Url where the ad is.
        xpaths: xpaths for the given type of ad.
        headers: Request headers.
        uuid: UUID of the ad.
        store_html: Store entire HTML of the URL.

    Returns:
        A dictionary containing all scraped data as HTML elements.
        This dict represents an row in the CSV result file.
    """
    response = requests_wrapper(url=url, headers=headers)

    if not response:
        return

    tree = etree.HTML(response.text)

    result_dict = {
        'url': url,
        'scrape_time': datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
        'uuid': uuid
    }

    if store_html:
        result_dict['html'] = response.text

    for k, v in xpaths.items():
        content = tree.xpath(v)

        if not content:
            result_dict[k] = ''
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


def iterate_pages(headers: dict,
                  toggles: list,
                  base_url: str,
                  max_pages=100) -> Generator[str]:
    """Iterate one page filled with ads.

    Iterate pages goes through all pages and extract ads
    to scrape. If no ads are found on the page, the function
    returns. The function stores all data in batches at the
    end of the page scrape.

    Args:
        headers: Request headers.
        toggles: A list of all possible checkbox toggles.
        base_url: Base URL used repetitively.
        max_pages: Max number of pages for each filter.

    Returns:
        URL of one AD to be scraped.
    """
    ad_pattern = re.compile(r'(\/stillinger\/stilling\/.+)')
        
    for t in toggles:
        logging.info(f'SCRAPING ARBEIDSPLASSEN WITH {t}')
        for page_number in range(max_pages):
            logging.info(f'SCRAPING PAGE {page_number + 1}')

            time.sleep(random.uniform(1.5, 2.5))

            if page_number == 0:
                url = f'{base_url}/stillinger?size=100&{t}&v=2'
            else:
                url = f'{base_url}/stillinger?from={page_number * 100}&size=100&{t}&v=2'
            
            response = requests_wrapper(url=url, headers=headers)

            if not response:
                continue

            headers['Referer'] = url
            soup = BeautifulSoup(response.text, 'html.parser')

            a_tags = soup.find_all('a')
            
            all_urls = [u for u in a_tags if isinstance(u, str)]
            all_urls = [u.get('href') for u in a_tags]

            ad_urls = [base_url + u for u in all_urls if ad_pattern.search(u)]

            if not ad_urls:
                logging.info('NO ADS ON PAGE')
                break
            
            yield from ad_urls

        logging.info(f'FINISHED SCRAPING ARBEIDSPLASSEN WITH {t}')
    

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

        response = requests_wrapper(url=url, headers=headers, raise_error=True)

        soup = BeautifulSoup(response.text, 'html.parser')
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
    uuid_pattern = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}')
    xpaths = arbeidsplassen_xpaths()
    toggles = get_toggles(flags['full_scrape'], flags['base_url'], headers)
    buffer_data = []

    if flags['ignore_previously_scraped']:
        scraped_urls = previously_scraped(scrape_folder=flags['scrape_folder'], n_files=50)
    else:
        scraped_urls = []
    
    for url in iterate_pages(headers=headers, toggles=toggles, base_url=flags['base_url']):
        uuid = uuid_pattern.search(url).group(0)

        # Check if ad has been scraped before.
        if uuid in scraped_urls:
            logging.info(f'ALREADY SCRAPED: {url}')
            continue

        scraped_urls.append(uuid)
        result = scrape_single_ad(url=url,
                                  headers=headers,
                                  xpaths=xpaths,
                                  uuid=uuid,
                                  store_html=flags['store_html'])

        if not result:
            continue
        
        buffer_data.append(result)
        time.sleep(random.uniform(flags['time_sleep_lower'], flags['time_sleep_upper']))

        if len(buffer_data) >= flags['buffer_size']:
            processed_ads = process_data(buffer_data)
            store_data(processed_ads, flags['scrape_folder'], curr_time)
            buffer_data = []


if __name__ == "__main__":
    main()
