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

def parse_job_data_from_soup(page_soup):
    jobs = page_soup.find_all("div", class_="your_class")
    job_data = []
    for job in jobs:
        job = BeautifulSoup(str(job), 'html.parser')
        row1 = job.find('div', class_="your_class")
        row2 = job.find('div', class_="your_class")
        row3 = job.find('div', class_="your_class")
        
        job_title = row1.a.text.strip() 
        company_name = row2.span.a.text.strip() 
        job_details = row3.find('div', class_="your_class") 
        location = job_details.find('span', class_="your_class").span.span.text.strip()
        job_url_element = job.find('a', class_= "your_class")
        url = job_url_element.get('href') if job_url_element else 'N/A'
        if job_url_element is None:
            logging.warning("URL not found for a job entry.")
    
        job_data.append({
            "job_title": job_title,
            "company_name": company_name,
            "location": location,
            "job_url": url
        })
    return job_data



def create_dataframe_of_job_data(job_data: List[Dict[str, str]]) -> pd.DataFrame:
    try:
        if job_data:
            column_names = ["job_title", "company_name", "location","job_url"]
            df = pd.DataFrame(job_data, columns=column_names)
            logging.info("Data converted into dataframe")
            return df
        else:
            logging.info("No job data found to create dataframe.")
            return pd.DataFrame(columns=["job_title", "company_name", "location","job_url"])
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def get_unique_companies_df(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    try:
        filtered_df = df.drop_duplicates(subset=[column_name]).reset_index(drop=True)
        logging.info("Unique company name dataframe created")
        return filtered_df
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def save_job_data_dataframe_to_mysql(df: pd.DataFrame) -> None:
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        utc_datetime = get_current_utc_datetime()
        date, time = extract_utc_date_and_time(utc_datetime)

        for index, row in df.iterrows():
            sql = "INSERT INTO naukri.job_data (DATE, TIME, job_title, company_name, location, job_url) VALUES (%s,%s,%s,%s,%s,%s)"
            values = (date, time, row['job_title'], row['company_name'], row['location'], row['job_url'])
            cursor.execute(sql, values)
            logging.info("Job details saved in DB successfully")

        mydb.commit()
        cursor.close()
        mydb.close()
        logging.info("DB closed successfully")
    except Exception as e:
        logging.error(f"An error occurred while saving job data to MySQL: {e}")
        raise CustomException(f"An error occurred while saving job data to MySQL: {e}")


def save_filtered_job_data_dataframe_to_mysql(df: pd.DataFrame) -> None:

    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        utc_datetime = get_current_utc_datetime()
        date, time = extract_utc_date_and_time(utc_datetime)

        for index, row in df.iterrows():
            sql = "INSERT INTO naukri.job_filtered_data (DATE, TIME, job_title, company_name, location, job_url) VALUES (%s,%s,%s,%s,%s,%s)"
            values = (date, time, row['job_title'], row['company_name'], row['location'], row['job_url'])
            cursor.execute(sql, values)
            logging.info("Job details saved in DB successfully")

        mydb.commit()
        cursor.close()
        mydb.close()
        logging.info("DB closed successfully")
    except Exception as e:
        logging.error(f"An error occurred while saving job data to MySQL: {e}")
        raise CustomException(f"An error occurred while saving job data to MySQL: {e}")
    

def extract_job_data_from_DB():
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)
        
        # Use timezone-aware datetime for threshold calculation
        threshold_datetime = datetime.now(timezone.utc) - timedelta(seconds=20)
        threshold_time_str = threshold_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        query = "SELECT * FROM naukri.job_data WHERE TIMESTAMP(CONCAT(DATE, ' ', TIME)) >= %s"
        cursor.execute(query, (threshold_time_str,))
        fetched_data = cursor.fetchall()
        
        logging.info("Data fetched successfully")
        
        cursor.close()
        mydb.close()
        logging.info("DB closed")
        
        return fetched_data
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)

   
 
def save_job_data_to_csv(job_data: List):
    try:
        if job_data:
            column_names = ["ID", "DATE", "TIME","job_title", "company_name", "location", "job_url"]
            df = pd.DataFrame(job_data, columns=column_names)
       
            folder_name = "Job_Data"
            os.makedirs(folder_name, exist_ok=True)
 
            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")
 
            df.to_csv(csv_file_path, index=False)
            logging.info("fetched job data saved in in CSV file successfully")
            return csv_file_path
        else:
            logging.info("No recent data found to save.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)