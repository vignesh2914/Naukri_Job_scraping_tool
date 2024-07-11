import streamlit as st
import pandas as pd
from scraper import (
    scrape_job_data, create_dataframe_of_job_data, get_unique_companies_df,
    save_job_data_dataframe_to_mysql, save_filtered_job_data_dataframe_to_mysql,
    extract_job_data_from_DB, save_job_data_to_csv, extract_unique_job_data_from_db,
    save_unique_job_data_to_csv, configure
)

configure()

st.title("Job Data Scraper Application - Datanetiix")

job_keyword = st.text_input("Job Keyword")
location_keyword = st.text_input("Location Keyword")
time_limit = st.number_input("Time Limit (seconds)", min_value=1, value=60)

if st.button("Submit"):
    if job_keyword and location_keyword and time_limit > 0:
        try:
            job_data = scrape_job_data(job_keyword, location_keyword, time_limit)
            
            if job_data:
                st.success("Job data scraped successfully!")
                df = create_dataframe_of_job_data(job_data)
                st.write("All Job Data")
                st.dataframe(df)
                
                if not df.empty:
                    save_job_data_dataframe_to_mysql(df)
                    st.success("All job data saved to MySQL!")
                
                unique_df = get_unique_companies_df(df, "company_name")
                st.write("Unique Job Data")
                st.dataframe(unique_df)
                
                if not unique_df.empty:
                    save_filtered_job_data_dataframe_to_mysql(unique_df)
                    st.success("Unique job data saved to MySQL!")
                
                if job_data:
                    non_filtered_csv_file = save_job_data_to_csv(job_data)
                    with open(non_filtered_csv_file, 'r') as file:
                        non_filtered_csv_content = file.read()
                    st.session_state['non_filtered_csv_content'] = non_filtered_csv_content

                unique_job_data = extract_unique_job_data_from_db()
                if unique_job_data:
                    filtered_csv_file = save_unique_job_data_to_csv(unique_job_data)
                    with open(filtered_csv_file, 'r') as file:
                        filtered_csv_content = file.read()
                    st.session_state['filtered_csv_content'] = filtered_csv_content

                st.success("Job scraping data saved successfully!")
            else:
                st.warning("No job data found.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.error("Please provide valid inputs for all fields.")

if 'non_filtered_csv_content' in st.session_state and 'filtered_csv_content' in st.session_state:
    st.download_button(
        label="Download All Job Data",
        data=st.session_state['non_filtered_csv_content'],
        file_name=f'{job_keyword}_{location_keyword}_job_data.csv',
        mime='text/csv',
        key='download-all-job-data'
    )

    st.download_button(
        label="Download Filtered Job Data",
        data=st.session_state['filtered_csv_content'],
        file_name=f'{job_keyword}_{location_keyword}_filtered_job_data.csv',
        mime='text/csv',
        key='download-filtered-job-data'
    )

if st.button("Refresh Data"):
    if 'non_filtered_csv_content' in st.session_state:
        del st.session_state['non_filtered_csv_content']
    if 'filtered_csv_content' in st.session_state:
        del st.session_state['filtered_csv_content']
    st.experimental_rerun()

