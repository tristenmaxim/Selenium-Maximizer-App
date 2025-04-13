from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import datetime
import os
import glob
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Constants and configuration
EMAIL = "x.maxtisten@gmail.com"
PASSWORD = "d338fa1f-fe54-4a74-b4f9-5cc918050a65"
DOWNLOAD_DIR = "/Users/maximtristen/Desktop/Selenium | Maximizer app/Downloads"
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), 'poised-shuttle-454415-t3-445540f86d93.json')
ARCHIVE_FOLDER_ID = "1kg9c9p6Vc802iWYZfK3XFX59ACN49Cb4"
UPLOAD_FOLDER_ID = "1gFIMVrhOKgfRZZiE7bzn1fvheBJIAaP5"

# Ensure the download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Function to archive previous files
def archive_previous_files(drive_service):
    print("Archiving previous files from Upload folder to Archive folder...")
    
    # Initialize an empty list to store all files
    all_files = []
    page_token = None
    
    # Use pagination to get ALL files in the folder
    while True:
        # Get a page of files
        query = f"'{UPLOAD_FOLDER_ID}' in parents"
        results = drive_service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        
        # Add this page of files to our list
        page_files = results.get('files', [])
        all_files.extend(page_files)
        
        # Get the next page token, if any
        page_token = results.get('nextPageToken')
        
        # If there are no more pages, exit the loop
        if not page_token:
            break
    
    if not all_files:
        print("No files found in Upload folder to archive.")
    else:
        print(f"Found {len(all_files)} files to archive.")
        for item in all_files:
            print(f"Moving file: {item['name']}")
            try:
                drive_service.files().update(
                    fileId=item['id'],
                    addParents=ARCHIVE_FOLDER_ID,
                    removeParents=UPLOAD_FOLDER_ID
                ).execute()
                print(f"Successfully moved: {item['name']}")
            except Exception as e:
                print(f"Error moving file {item['name']}: {str(e)}")
        
        # Verify that all files have been moved
        verification = drive_service.files().list(
            q=f"'{UPLOAD_FOLDER_ID}' in parents",
            fields="files(id, name)"
        ).execute()
        
        remaining_files = verification.get('files', [])
        if remaining_files:
            print(f"WARNING: {len(remaining_files)} files still remain in Upload folder after archiving:")
            for file in remaining_files:
                print(f" - {file['name']}")
            print("Attempting to move remaining files...")
            for item in remaining_files:
                try:
                    drive_service.files().update(
                        fileId=item['id'],
                        addParents=ARCHIVE_FOLDER_ID,
                        removeParents=UPLOAD_FOLDER_ID
                    ).execute()
                    print(f"Successfully moved remaining file: {item['name']}")
                except Exception as e:
                    print(f"Error moving remaining file {item['name']}: {str(e)}")
        else:
            print("Verification complete: Upload folder is now empty.")

def download_tiktok_data():
    print("Starting TikTok data download process...")
    
    # Setup Chrome options for auto download
    chrome_options = Options()
    # Add headless mode
    chrome_options.add_argument("--headless=new")
    # These options help with downloading files in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    wait = WebDriverWait(driver, 20)
    
    downloaded_file_path = None

    try:
        # Step 1: Login
        driver.get("https://app.maximizer.io/login")
        
        # Wait for page to fully load
        time.sleep(5)
        
        # Use the exact ID selectors from the form
        email_field = wait.until(EC.presence_of_element_located((By.ID, "fieldEmail")))
        email_field.send_keys(EMAIL)
        
        password_field = wait.until(EC.presence_of_element_located((By.ID, "fieldPassword")))
        password_field.send_keys(PASSWORD)
        
        # Find the sign in button
        login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]')))
        login_button.click()
        
        # Ensure we're logged in by waiting for dashboard
        wait.until(EC.url_contains("dashboard"))
        print("Successfully logged in!")

        # Step 2: Navigate directly to the TikTok reporting page with dynamic dates
        wait.until(EC.url_contains("dashboard"))
        
        # Calculate date range (last 3 days including today)
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=2)
        
        # Construct the full TikTok reporting URL with dynamic dates
        tiktok_report_url = (
            "https://app.maximizer.io/reporting/tiktok/"
            "?dimensions=date%2Csource_name%2Caccount_id%2Ccampaign_name%2Cadgroup_id%2C"
            "adgroup_name%2Cadgroup_operation_status%2Cadgroup_secondary_status"
            "&metrics=impressions%2Cclicks%2Cspend%2Ccpa%2Cviews%2Cconversions%2Crevenues%2C"
            "rpm%2Crpc%2Cprofit%2Croi"
            "&limit=250&page=1&sort=adgroup_id&order=ASC&filters=W10%3D"
            f"&dateStart={start_date}&dateEnd={end_date}"
        )
        
        # Print the URL to verify it's correct
        print(f"Navigating to: {tiktok_report_url}")
        driver.get(tiktok_report_url)
        
        # Wait for the report page to load
        time.sleep(5)

        # Step 3: Click export button using a valid CSS selector
        print("Attempting to click export button...")
        export_btn_selector = "button.export-button, button[title='Export'], button:contains('Export')"
        try:
            # Try first selector
            export_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.export-button")))
            export_button.click()
            print("Export button clicked (first method)")
        except:
            try:
                # Try second selector
                export_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export')]")))
                export_button.click()
                print("Export button clicked (second method)")
            except:
                # Try third selector
                export_button = driver.find_element(By.CSS_SELECTOR, "button[title='Export']")
                export_button.click()
                print("Export button clicked (third method)")

        # Step 4: Navigate to exports and download file
        print("Navigating to exports page...")
        driver.get("https://app.maximizer.io/exports")
        
        # Wait for the exports page to load and find the first download button
        download_btn_selector = "a.btn.btn-sm.btn-primary[href*='/api/reporting/exports/'][target='_blank']"
        print("Clicking download button...")
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, download_btn_selector))).click()

        # Wait to ensure download completes
        print("Waiting for download to complete...")
        time.sleep(15)  # Increased wait time for download
        
        # Check if any file has been downloaded
        files_before_download = set(os.listdir(DOWNLOAD_DIR))
        
        # Keep checking for new files for up to 30 seconds
        max_wait_time = 30
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            current_files = set(os.listdir(DOWNLOAD_DIR))
            new_files = current_files - files_before_download
            csv_files = [f for f in new_files if f.endswith('.csv')]
            
            if csv_files:
                print(f"Download detected: {len(csv_files)} new CSV files")
                break
            
            print("Waiting for download to appear...")
            time.sleep(2)
        
        # Find the most recently downloaded file in the download directory
        csv_files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.csv')]
        if csv_files:
            most_recent_file = max(csv_files, key=os.path.getctime)
            
            # Create new filename with TikTok-{date}{time} format
            now = datetime.datetime.now()
            date_str = now.strftime("%Y%m%d-%H%M%S")
            new_filename = os.path.join(DOWNLOAD_DIR, f"TikTok-{date_str}.csv")
            
            # Rename the file
            os.rename(most_recent_file, new_filename)
            print(f"File renamed to: {new_filename}")
            
            downloaded_file_path = new_filename
        else:
            print("No CSV files found in download directory!")

    finally:
        driver.quit()
        
    return downloaded_file_path

def upload_to_drive(downloaded_file_path):
    print(f"Starting upload process for file: {downloaded_file_path}")
    
    if not os.path.exists(downloaded_file_path):
        print(f"Error: File {downloaded_file_path} does not exist!")
        return False
    
    # Set up Google API credentials
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    gc = gspread.authorize(creds)
    
    # Execute archiving as the first step
    archive_previous_files(drive_service)
    
    # Get filename from path
    filename = os.path.basename(downloaded_file_path)
    
    # Rename clearly with today's date
    now = datetime.datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H-%M-%S')
    new_filename = f"TikTok-stats-{today_str}-{time_str}.csv"
    new_filepath = os.path.join(DOWNLOAD_DIR, new_filename)
    os.rename(downloaded_file_path, new_filepath)
    print(f"File renamed for upload: {new_filename}")
    
    # Upload new CSV clearly to Google Drive
    file_metadata = {'name': new_filename, 'parents': [UPLOAD_FOLDER_ID]}
    media = MediaFileUpload(new_filepath, mimetype='text/csv')
    uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"File uploaded to Google Drive with ID: {uploaded_file.get('id')}")
    
    # Data preprocessing (convert to numeric)
    df = pd.read_csv(new_filepath)
    
    # Remove unwanted characters and convert clearly to numeric
    numeric_cols = ['spend', 'cpa', 'revenues', 'rpm', 'rpc', 'profit', 'roi']
    for col in numeric_cols:
        df[col] = df[col].replace({'\$':'', ',':'', '%':''}, regex=True).astype(float)
    
    # Ensure date is correctly formatted
    df['date'] = pd.to_datetime(df['date'])
    
    # Convert DataFrame to JSON serializable format
    # Convert Timestamp objects to strings before sending to Google Sheets
    df_for_sheets = df.copy()
    df_for_sheets['date'] = df_for_sheets['date'].dt.strftime('%Y-%m-%d')  # Convert timestamps to strings
    
    # Create Google Sheet and upload clean data
    sheet_name = f'TikTok Pivot {today_str} {time_str}'
    sheet = gc.create(sheet_name, folder_id=UPLOAD_FOLDER_ID)
    worksheet = sheet.get_worksheet(0)
    worksheet.update([df_for_sheets.columns.values.tolist()] + df_for_sheets.values.tolist())
    print(f"Data uploaded to new Google Sheet: {sheet_name}")
    
    # Add timestamp information to the sheet
    timestamp_info = f"Created on: {now.strftime('%Y-%m-%d %H:%M:%S')}"
    worksheet.update_cell(len(df) + 3, 1, timestamp_info)
    
    # Create Pivot Table clearly with correct indexing
    pivot_sheet = sheet.add_worksheet(title='Pivot Table', rows="100", cols="20")
    
    # Find column indices based on column names
    column_indices = {col: idx for idx, col in enumerate(df.columns)}
    
    pivot_body = {
        "requests": [{
            "updateCells": {
                "rows": [{
                    "values": [{"pivotTable": {
                        "source": {
                            "sheetId": worksheet.id,
                            "startRowIndex": 0,
                            "startColumnIndex": 0,
                            "endRowIndex": len(df) + 1,
                            "endColumnIndex": len(df.columns)
                        },
                        "rows": [
                            {
                                "sourceColumnOffset": column_indices["source_name"],
                                "sortOrder": "DESCENDING",
                                "showTotals": True
                            },
                            {
                                "sourceColumnOffset": column_indices["adgroup_operation_status"],
                                "sortOrder": "DESCENDING", 
                                "showTotals": True
                            },
                            {
                                "sourceColumnOffset": column_indices["adgroup_id"],
                                "sortOrder": "DESCENDING",
                                "showTotals": True
                            },
                            {
                                "sourceColumnOffset": column_indices["date"],
                                "sortOrder": "ASCENDING",
                                "showTotals": True
                            }
                        ],
                        # No columns section - we don't want any columns in the pivot
                        "values": [
                            {
                                "summarizeFunction": "SUM",
                                "sourceColumnOffset": column_indices["spend"],
                                "name": "Total Spend"
                            },
                            {
                                "summarizeFunction": "SUM",
                                "sourceColumnOffset": column_indices["conversions"],
                                "name": "Total Conversions"
                            },
                            {
                                "summarizeFunction": "AVERAGE",
                                "sourceColumnOffset": column_indices["cpa"],
                                "name": "Average CPA"
                            },
                            {
                                "summarizeFunction": "AVERAGE",
                                "sourceColumnOffset": column_indices["rpc"],
                                "name": "Average RPC"
                            },
                            {
                                "summarizeFunction": "SUM",
                                "sourceColumnOffset": column_indices["profit"],
                                "name": "Total Profit"
                            },
                            {
                                "summarizeFunction": "AVERAGE",
                                "sourceColumnOffset": column_indices["roi"],
                                "name": "Average ROI"
                            }
                        ]
                    }}]
                }],
                "start": {"sheetId": pivot_sheet.id, "rowIndex": 0, "columnIndex": 0},
                "fields": "pivotTable"
            }
        }]
    }
    
    sheet.batch_update(pivot_body)
    print("Pivot table created successfully")
    
    # Add professional blue theme to pivot table
    blue_theme_request = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": pivot_sheet.id
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.87,
                            "green": 0.91,
                            "blue": 0.97
                        },
                        "textFormat": {
                            "foregroundColor": {
                                "red": 0,
                                "green": 0,
                                "blue": 0
                            },
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        }]
    }
    
    sheet.batch_update(blue_theme_request)
    print("Blue theme applied to pivot table")
    
    return True

# Main execution
if __name__ == "__main__":
    print("Starting TikTok data download and upload process...")
    
    # First download the data
    downloaded_file = download_tiktok_data()
    
    if downloaded_file and os.path.exists(downloaded_file):
        print(f"Download successful! File saved at: {downloaded_file}")
        
        # Then upload to Google Drive
        upload_success = upload_to_drive(downloaded_file)
        
        if upload_success:
            print("✅ Process completed successfully - data downloaded, uploaded to Google Drive, and pivot table created!")
        else:
            print("❌ Upload process failed. Please check the logs for details.")
    else:
        print("❌ Download process failed or file not found. Cannot proceed with upload.")