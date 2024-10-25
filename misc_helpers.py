import os
import sys
import logging
import re

import pandas as pd
from datetime import datetime
from random import choice


def arbeidsplassen_xpaths() -> dict:
    """XPATHS used to identify where the content for an ad is located."""
    xpaths = {
        'title': '//*[@id="main-content"]/article/div/h1',
        'company': '//*[@id="main-content"]/article/div/section[1]/div[1]/p',
        'location': '//*[@id="main-content"]/article/div/section[1]/div[2]/p',
        'job_content': '//div[contains(@class, "job-posting-text")]',
        'employer': '//h2[contains(text(), "Om bedriften")]/../div',
        'deadline': '//h2[contains(text(), "Søk på jobben")]/../p',
        'about': '//h2[contains(text(), "Om jobben")]/../../dl',
        'contact_person': '//h2[contains(text(), "Kontaktperson for stillingen") or contains(text(), "Kontaktpersoner for stillingen")]/..',
        'ad_data': '//h2[contains(text(), "Annonsedata")]/../dl'
    }

    return xpaths


def load_random_headers():
    """Set up headers with a random user agent."""
    user_agents = [
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2859.0 Safari/537.36',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.49 Safari/537.36',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:55.0) Gecko/20100101 Firefox/55.0',
        'user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2876.0 Safari/537.36',
        'user-agent=Mozilla/5.0 (X11; Linux i686 (x86_64)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3187.0 Safari/537.366',
        'user-agent=Mozilla/5.0 (X11;Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3178.0 Safari/537.36',
        'user-agent=Mozilla/5.0 (X11; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
    ]

    headers = {
        "User-Agent": choice(user_agents),
        "Accept-Language": "en-GB,en,q=0.5",
        "Referer": "https://google.com",
        "DNT": "1"
    }

    return headers


def init_logging(filename):
    """Initiate a logging file in folder logs"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)


def extract_datetime(filename: str) -> str:
    """Extract datetime as string from a given filename."""
    date_and_time = re.search(r'(\d{4}_\d{2}_\d{2})', filename)

    if not date_and_time:
        return None

    parsed_datetime = datetime.strptime(date_and_time[0], '%Y_%m_%d')
    parsed_datetime = parsed_datetime.strftime('%Y_%m_%d')

    return parsed_datetime


def previously_scraped(scrape_folder: str,
                       n_files: str):
    """
    Checks previously scraped files to avoid scraping the same
    ad multiple times. The number of files to check can be customized.
    The files are then sorted by datetime.

    Args:
        dirpath: Directory for previously scraped csvs.
        n_files: How many files to account for.
    Returns:
        A list of with identifiers. For finn the id is finn_code.
    """

    metadata = []

    filenames = os.listdir(scrape_folder)
    filenames = sorted(filenames, key=extract_datetime)
    filenames = filenames[-n_files:]

    for f in filenames:

        with open(f'{scrape_folder}/{f}', encoding='utf-8') as file:
            n_ads = sum(1 for _ in file)

        if n_ads == 0:
            os.remove(f'{scrape_folder}/{f}')
            continue

        row = {
            'filename': f,
            'datetime': extract_datetime(f),
            'n_ads': n_ads
        }

        metadata.append(row)

    # No previous scrapes
    if not metadata:
        return []

    files = [d['filename'] for d in metadata]

    previous_scrapes = pd.concat(
        (pd.read_csv(f'{scrape_folder}/{f}', encoding='utf-8') for f in files if os.path.isfile(f'{scrape_folder}/{f}')),
        ignore_index=True)

    scraped_codes = previous_scrapes['uuid'].astype(str).to_list()

    return scraped_codes
