import logging
import sys
import pandas as pd
from exception import CustomException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from selenium_stealth import stealth
from typing import List, Dict
import os
from database import connect_to_mysql_database, create_cursor_object
from dotenv import load_dotenv
from utils import get_current_utc_datetime, extract_utc_date_and_time


def configure():
    load_dotenv()


configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")


def make_url(job_keyword: str, location_keyword: str, index: int) -> str:
    formatted_job_keyword = job_keyword.replace(" ", "-")
    formatted_location_keyword = location_keyword.replace(" ", "-")
    base_url = "https://www.naukri.com/{}-jobs-in-{}-{}"
    url = base_url.format(formatted_job_keyword, formatted_location_keyword, index)

    logging.info(f"Scraping URL: {url}")
    return url


def scrape_job_data(job_keyword: str, location_keyword: str, time_limit: int) -> List[Dict[str, str]]:
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--log-level=3")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
    )
    
    start_time = datetime.now()
    logging.info('Crawl starting time: {}'.format(start_time.time()))
    end_time = start_time + timedelta(seconds=time_limit)
    
    all_jobs = []
    page_number = 1

    while datetime.now() < end_time:
        url = make_url(job_keyword, location_keyword, page_number)
        driver.get(url)
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "srp-jobtuple-wrapper")))
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            page_jobs = parse_job_data_from_soup(soup)
            all_jobs.extend(page_jobs)
        except:
            logging.error("Loading took too much time or no jobs found!")
            break
        
        page_number += 1

    driver.quit()
    final_end_time = datetime.now()
    logging.info('Crawl ending time: {}'.format(final_end_time.time()))
    logging.info('Total time taken: {}'.format(final_end_time - start_time))

    return all_jobs
