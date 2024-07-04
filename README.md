
# Naukri Web Scraper Project
This project automates the process of scraping job listings from Naukri.com using Selenium and Beautiful Soup, then processes and stores the data in a structured format. Below is a detailed workflow of the project.

# Project Workflow

- Step 1: Scraping Data
Using Selenium and Beautiful Soup, we scrape job listings and store them in a DataFrame. Key packages utilized:

Selenium: Automates browser interaction for scraping dynamic content.
Beautiful Soup: Parses HTML to extract job details.

- Step 2: Import Data to Database
The scraped data, stored in the DataFrame, is imported into a database for further processing and storage.

- Step 3: Export Data as CSV
From the database, the data is exported and saved as CSV files in two folders:

Filtered Data: Contains data filtered by repeated company names.
Job Data: Contains unfiltered job listings.

Ensure you have the following installed:

- Python 3.x
- Selenium
- Beautiful Soup 4
- Pandas
- SQLAlchemy (or any preferred DB library)