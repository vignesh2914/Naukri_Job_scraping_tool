import logging
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium_stealth import stealth
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def make_url(job_keyword: str, location_keyword: str, index: int) -> str:
    formatted_job_keyword = job_keyword.replace(" ", "-")
    formatted_location_keyword = location_keyword.replace(" ", "-")
    base_url = "https://www.naukri.com/{}-jobs-in-{}-{}"
    url = base_url.format(formatted_job_keyword, formatted_location_keyword, index)

    logging.info(f"Scraping URL: {url}")
    return url

def scrape_job_data(url: str):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920x1080')
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
    
    driver.get(url)
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "srp-jobtuple-wrapper")))
        page_source = driver.page_source
        driver.quit()
        return page_source
    except:
        logging.error("Loading took too much time!")
        driver.quit()
        return None

def parse_job_data_from_soup(page_soup):
    jobs = page_soup.find_all("div", class_="Your class")
    job_data = []
    for job in jobs:
        job = BeautifulSoup(str(job), 'html.parser')
        row1 = job.find('div', class_="your class")
        row2 = job.find('div', class_="your class")
        row3 = job.find('div', class_="your class")

        print(job)
        
        job_title = row1.a.text.strip()
        if row1 is None or row1.a is None:
            logging.warning("Job title not found for a job entry.")
            
        company_name = row2.span.a.text.strip() 
        if row2 is None or row2.span is None or row2.span.a is None:
            logging.warning("Company name not found for a job entry.")
            
        job_details = row3.find('div', class_="your class")
        location = job_details.find('span', class_="your class").span.span.text.strip() if job_details and job_details.find('span', class_="your class") and job_details.find('span', class_="your_class").span and job_details.find('span', class_="your_class").span.span else 'N/A'
        if job_details is None or job_details.find('span', class_="your class") is None or job_details.find('span', class_="your class").span is None or job_details.find('span', class_="your_class").span.span is None:
            logging.warning("Location not found for a job entry.")
        
        job_url_element = job.find('a', class_= "title")
        url = job_url_element.get('href') if job_url_element else 'N/A'
        if job_url_element is None:
            logging.warning("URL not found for a job entry.")
    
        job_data.append({
            "ROLE": job_title,
            "COMPANY_NAME": company_name,
            "LOCATION": location,
            "url": url
        })
    return job_data

def create_dataframe_of_job_data(job_data: List[Dict[str, str]]) -> pd.DataFrame:

    try:
        if job_data:
            column_names = ["ROLE", "COMPANY_NAME", "LOCATION", "url"]
            df = pd.DataFrame(job_data, columns=column_names)
            logging.info("Data converted into dataframe")
            return df
        else:
            logging.info("No job data found to create dataframe.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def get_unique_companies_df(df: pd.DataFrame, column_name: str) -> pd.DataFrame:

    try:
        filtered_df = df.drop_duplicates(subset=[column_name]).reset_index(drop=True)
        logging.info("Unique company name dataframe created")
        return filtered_df
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def main():
    job_keyword = input("Enter the job keyword: ")
    location_keyword = input("Enter the location: ")
    time_limit = int(input("Enter the seconds to scrape: "))

    start_time = datetime.now()
    logging.info('Crawl starting time: {}'.format(start_time.time()))
    logging.info("")

    all_jobs = []
    page_number = 1
    end_time = start_time + timedelta(seconds=time_limit)

    while datetime.now() < end_time:
        url = make_url(job_keyword, location_keyword, page_number)
        page_source = scrape_job_data(url)
        
        if page_source:
            soup = BeautifulSoup(page_source, 'html.parser')
            page_jobs = parse_job_data_from_soup(soup)
            all_jobs.extend(page_jobs)
        else:
            break
        
        page_number += 1

    final_end_time = datetime.now()
    logging.info('Crawl ending time: {}'.format(final_end_time.time()))
    logging.info('Total time taken: {}'.format(final_end_time - start_time))

    job_df = create_dataframe_of_job_data(all_jobs)
    unique_companies_df = get_unique_companies_df(job_df, "COMPANY_NAME")
    
    print("Job DataFrame:")
    print(job_df)
    print("\nUnique Companies DataFrame:")
    print(unique_companies_df)

if __name__ == "__main__":
    main()











# def main(job_keyword="python", location_keyword="india", time_limit=10):
#     job_data = scrape_job_data(job_keyword, location_keyword, time_limit)
#     job_df = create_dataframe_of_job_data(job_data)
#     unique_companies_df = get_unique_companies_df(job_df, "company_name")


#     print(job_df)
#     print(unique_companies_df)
#     save_job_data_dataframe_to_mysql(job_df)
#     save_filtered_job_data_dataframe_to_mysql(unique_companies_df)
    
#     fetched_job_data = extract_job_data_from_DB()
#     save_job_data_to_csv(fetched_job_data)

#     fetched_filtered_job_data = extract_unique_job_data_from_db()
#     save_unique_job_data_to_csv(fetched_filtered_job_data)
    

# if __name__ == "__main__":
#     main()
