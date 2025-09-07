import os
import sys
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def authenticate_google_drive():
    """
    Authenticates using a service account key. THIS IS THE SERVER VERSION.
    It does not open a browser and is meant for GitHub Actions.
    """
    gauth = GoogleAuth()
    
    # This tells PyDrive2 the name of the JSON key file that our
    # GitHub Actions workflow creates from a secret.
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = 'service_secrets.json'
    
    # This is the correct command for a non-interactive, server-based login.
    gauth.ServiceAuth()
    
    drive = GoogleDrive(gauth)
    return drive

def get_or_create_folder(drive, folder_name, parent_folder_id):
    """Checks for a folder, creates it if not found, and returns its ID."""
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and title='{folder_name}' and trashed=false"
    file_list = drive.ListFile({'q': query, 'supportsAllDrives': True, 'includeItemsFromAllDrives': True}).GetList()
    if file_list:
        print(f"  - Found existing folder: '{folder_name}'")
        return file_list[0]['id']
    else:
        print(f"  - Folder '{folder_name}' not found. Creating...")
        folder_metadata = {'title': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': parent_folder_id}]}
        folder = drive.CreateFile(folder_metadata)
        folder.Upload(param={'supportsAllDrives': True})
        return folder['id']

def upload_to_drive(drive, file_path, company_code, report_type):
    """Uploads a file to the correct nested folder structure in Google Drive."""
    print(f"\n  - Uploading to Google Drive path: /CSE Reports/{company_code}/{report_type} Reports")
    root_folder_id = get_or_create_folder(drive, "CSE Reports", "root")
    company_folder_id = get_or_create_folder(drive, company_code, root_folder_id)
    report_type_folder_name = f"{report_type} Reports"
    destination_folder_id = get_or_create_folder(drive, report_type_folder_name, company_folder_id)

    file_name = os.path.basename(file_path)
    drive_file = drive.CreateFile({'title': file_name, 'parents': [{'id': destination_folder_id}]})
    drive_file.SetContentFile(file_path)
    drive_file.Upload(param={'supportsAllDrives': True})
    print(f"  - ✅ File '{file_name}' uploaded successfully.")

def download_report(driver, wait, company_code, report_type, drive, start_date_str):
    """Downloads reports of a specific type and triggers the upload process."""
    try:
        print("-" * 40)
        print(f"Starting process for {report_type} Reports...")
        if report_type == 'Quarterly': tab_name, table_id = 'Quarterly Reports', '21b'
        elif report_type == 'Annual': tab_name, table_id = 'Annual Reports ', '11b'
        else: return

        print(f"Step A: Clicking '{tab_name}' tab...")
        report_tab_element = wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{tab_name}')]")))
        driver.execute_script("arguments[0].click();", report_tab_element)
        wait.until(EC.visibility_of_element_located((By.XPATH, f"//div[@id='{table_id}']//table//tr")))
        time.sleep(1) 

        print("Step B: Parsing page for all report links...")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        report_container = soup.find('div', id=table_id)
        if not report_container: return

        all_report_rows = report_container.find('tbody').find_all('tr')
        if not all_report_rows: return
        
        cutoff_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        downloads_found = 0

        for report_row in all_report_rows:
            columns = report_row.find_all('td')
            if len(columns) < 2: continue
            uploaded_date_str = columns[0].contents[0].strip()
            try:
                date_obj = datetime.strptime(uploaded_date_str, '%d %b %Y')
            except ValueError: continue
            
            if date_obj >= cutoff_date:
                pdf_link_tag = report_row.find('a', href=lambda href: href and href.endswith('.pdf'))
                if not pdf_link_tag: continue
                
                download_url = pdf_link_tag['href']
                formatted_date = date_obj.strftime('%Y-%m-%d')
                
                temp_download_dir = "temp_reports"
                os.makedirs(temp_download_dir, exist_ok=True)
                local_filename = f"{formatted_date}-{company_code}.pdf"
                file_path = os.path.join(temp_download_dir, local_filename)

                print(f"\n  - Found valid report: {local_filename}")
                print(f"    - Downloading temporarily...")
                pdf_response = requests.get(download_url)
                pdf_response.raise_for_status() 
                with open(file_path, 'wb') as f:
                    f.write(pdf_response.content)

                upload_to_drive(drive, file_path, company_code, report_type)

                os.remove(file_path)
                downloads_found += 1
            else:
                print(f"  - Reached end of valid date range. No more reports to check.")
                break 

        if downloads_found == 0:
            print(f"\n  - No new {report_type} reports found after {cutoff_date.strftime('%Y-%m-%d')}.")

    except Exception as e:
        print(f"❌ An error occurred during the {report_type} report download: {e}")

def run_downloader(company_code, start_date_str):
    """The main function that orchestrates the entire process."""
    print("Step 1: Authenticating with Google Drive using Service Account...")
    try:
        drive = authenticate_google_drive()
        print("✅ Google Drive authentication successful.")
    except Exception as e:
        print(f"❌ Could not authenticate with Google Drive. Error: {e}")
        return

    base_url = "https://www.cse.lk/pages/company-profile/company-profile.component.html?symbol="
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 20)
        print(f"\n{'='*50}\nStarting scrape for company: {company_code}\n{'='*50}")
        company_url = base_url + company_code
        print("Navigating to the company page...")
        driver.get(company_url)
        print("Clicking 'Financials' tab...")
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Financials')]"))).click()
        time.sleep(2)
        
        download_report(driver, wait, company_code, 'Quarterly', drive, start_date_str)
        download_report(driver, wait, company_code, 'Annual', drive, start_date_str)

    except Exception as e:
        print(f"\n❌ A critical error occurred in the main process: {e}")
    finally:
        if driver: driver.quit()
        print("\nBrowser closed.")

if __name__ == "__main__":
    print("Script started.")
    if len(sys.argv) == 3:
        company_code_arg = sys.argv[1]
        start_date_arg = sys.argv[2]
        run_downloader(company_code_arg, start_date_arg)
    else:
        print("Usage: python scraper.py <COMPANY_CODE> <YYYY-MM-DD>")
    print("Script finished.")
