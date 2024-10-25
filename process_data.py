import pandas as pd

from bs4 import BeautifulSoup


def dl_to_dict(dl_html):

    dl_html = BeautifulSoup(dl_html, 'html.parser')
    dl_element = dl_html.find('dl')

    data_dict = {}
    for dt, dd in zip(dl_element.find_all('dt'), dl_element.find_all('dd')):
        data_dict[dt.get_text(strip=True)] = dd.get_text(strip=True)

    return data_dict


def divs_to_list(div_html):
    soup = BeautifulSoup(div_html, 'html.parser')
    keyword_divs = soup.find_all('div', class_='py-4 break-words')

    keywords = [div.get_text(strip=True) for div in keyword_divs]

    return keywords


def process_datapoint(datapoint):

    if not isinstance(datapoint, str):
        return datapoint

    elif any(datapoint.startswith(pre) for pre in ['<h1', '<h2', '<h3', '<h4',
                                                   '<section', '<p', '<div', '<span']):
        soup = BeautifulSoup(datapoint, 'html.parser')

        br_tags = soup.find_all('br')
        br_tags = [br_tag.replace_with(' ') for br_tag in br_tags]

        datapoint = soup.get_text()
        datapoint = datapoint.rstrip()

    elif datapoint.startswith('<dl'):
        datapoint = dl_to_dict(datapoint)

    return datapoint


def process_data(page_ads):

    processed_list = []

    for ad in page_ads:
        processed_ad = {}

        for key, value in ad.items():
            data = process_datapoint(value)

            if not isinstance(data, dict):
                data = {key: data}
            else:
                data = {key.lower().replace(' ', '_'): value for key, value in data.items()}


            processed_ad.update(data)
        
        processed_list.append(processed_ad)

    return processed_list

    
