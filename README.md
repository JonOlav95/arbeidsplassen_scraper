# About
Python script for scraping job advertisements from https://arbeidsplassen.nav.no/stillinger

The script includes the code to clean and store the data in CSV files. Sample data for 10 job advertisements can be found in logs/scrapes.


Use the code responsibly. The parameters `full_scrape` and `ignore_previously_scrape` are added as to not unnecessarily burden the server. 
# Setup
`pip install -r requirements.txt`

Only tested on Python 3.11

Run program: `python scr/main.py`

# Parameters
Parameters can be adjusted in `parameters.yml`

| Parameter | Type | Description |
| :---:   | :---: | :---: |
| full_scrape | boolean | If true, scrape every possible job advertisements. If false, scrape job advertisements from the last 24 hours. Should be False if the script is set up to run 2+ times a day. |
| ignore_previously_scraped | boolean | Pull down previously scraped job advertisements from the scrape folder and ignore them for the current scrape session. |
| scrape_folder | string | Which folder to store the scraped data. |
| log_folder | string | Which folder to store logs. |
| base_url | string | Base URL for arbeidsplassen (constant). |
| store_html | boolean | If true, store the entire HTML content of each job advertisement. If false, only store data represented in the xpaths function. |
| buffer_size | int | Number of job advertisements to store in memory before writing them to file. |
| time_sleep_lower | float | Minimum sleep length between scraping one job advertisement. |
| time_sleep_upper | float | Maximum sleep length between scraping one job advertisement. |


### Todos
- [ ] Dynamic headers (?).
- [ ] Support proxies.
- [ ] Daily automatic status update on github if xpaths/script is broken, as is the nature of web scraping.
- [ ] Extract elements susceptible to change from the code (e.g. url format, regex format).