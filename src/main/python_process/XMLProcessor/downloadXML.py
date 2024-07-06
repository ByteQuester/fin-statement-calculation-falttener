import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import os

from src.main.python_service.config import config



# Assuming these functions are part of the necessary modules

def get_filtered_data():
    """
    Connect to the database, join the sec_table and company_financials, and filter the data based on cik and year.
    Returns a DataFrame with the filtered data.
    """
    connection = pymysql.connect(
        host=config.DB_HOST_NAME,
        user=config.DB_USER,
        passwd=config.DB_PASSWORD,
        db=config.DB_NAME
    )


    query = """
    SELECT s.cik, s.year, s.url
    FROM sec_table s
    JOIN company_financials c ON s.cik = c.cik AND s.year = c.year
    """

    df = pd.read_sql(query, connection)
    connection.close()

    return df


def modify_url(url):
    """
    Modify the URL by replacing "-" with "" and ".txt" with "".
    """
    return url.replace("-", "").replace(".txt", "")


def download_cal_file(base_url, cik, year):
    """
    Downloads the first XML file ending in '_cal.xml' from the given URL.
    Saves the file in a hardcoded directory with a unique filename.
    Returns the file path if successful, or None if an error occurs.
    """
    modified_url = "https://www.sec.gov/Archives/"
    url = f"{modified_url}{base_url}"
    headers = {"User-Agent": "user@gmail.com"}  # Mimic browser request
    save_directory = "src/main/python_process/XMLProcessor/fetched_xml"  # Hardcoded save directory

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for HTTP errors

        soup = BeautifulSoup(response.text, "html.parser")
        xml_links = soup.find_all("a", href=re.compile(r".*_cal\.xml$"))

        if not xml_links:
            raise FileNotFoundError(f"No '_cal.xml' file found at {base_url}")

        cal_file_url = urljoin(url, xml_links[0]["href"])

        cal_file_response = requests.get(cal_file_url, headers=headers)
        cal_file_response.raise_for_status()

        # Define a unique filename and save path
        filename = f"cal_{cik}_{year}.xml"
        output_file = os.path.join(save_directory, filename)

        with open(output_file, "wb") as f:
            f.write(cal_file_response.content)

        return output_file

    except (requests.RequestException, FileNotFoundError) as e:
        print(f"Error downloading {base_url}: {e}")
        return None  # Return None on error


def rm_main(data):
    results = []

    for _, row in data.iterrows():
        modified_url = row["modified_url"]
        cik = row["cik"]
        year = row["year"]

        # Download the file and get the file path or None on error
        file_path = download_cal_file(modified_url, cik, year)

        if file_path:
            results.append({"modified_url": modified_url, "file_path": file_path})
        else:
            results.append({"modified_url": modified_url, "error": "Error downloading file"})

    return pd.DataFrame(results)


def main():
    # Get the filtered data
    df = get_filtered_data()

    # Modify the URL
    df['modified_url'] = df['url'].apply(modify_url)

    # Pass the filtered and modified data to the next script
    results_df = rm_main(df)

    # Save the results to a CSV file (optional)
    ##//results_df.to_csv('/Users/stewie/Desktop/fetch_xml/results.csv', index=False)
    ##//print("Process completed successfully.")


if __name__ == "__main__":
    main()
