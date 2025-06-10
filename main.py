from logger_config import logger
import dotenv
import os
import d2l_functions
import sys
import time
import datetime
from datetime import date

API_URL = "/d2l/api/lp/1.51/dataExport/"

def get_config():
    return {
        "bspace_url": os.environ["bspace_url"],
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_secret"],
        "scope": os.environ["scope"],
        "refresh_token": os.environ["refresh_token"],
        "dataset_id":os.environ["data_set_id"]
    }

def set_refresh_token(refresh_token):
    dotenv_file = dotenv.find_dotenv()
    os.environ["refresh_token"] = refresh_token
    dotenv.set_key(dotenv_file, "refresh_token", os.environ["refresh_token"])
    dotenv.load_dotenv(dotenv_file)


# Keep latest three reports and delete the rest
def delete_x_days_old_reports(path):
    files = []
    for filename in os.listdir(path):
        if "All%20Grades-" in filename and filename.endswith(".zip"):
            try:
                timestamp_str = filename.replace(".zip", "").split("T")[0].split("-")[-3:]
                date_str = "-".join(timestamp_str)
                file_date = datetime.datetime.strptime(date_str, "%m-%d-%Y").date()
                files.append((file_date, filename))
            except Exception as e:
                logger.error(f"Skipping file {filename}: {e}")

    # Sort by date descending (newest first)
    files.sort(reverse=True)

    # Keep the latest 3 files, delete the rest
    for _, filename in files[3:]:
        file_path = os.path.join(path, filename)
        try:
            os.remove(file_path)
            logger.info(f"Deleted old report: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")


# Main function
def main():
    logger.info("Started...")

    # Loading the dotfile
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

    config = get_config()
    base = 'downloads'
    os.makedirs(base, exist_ok=True)

    # Getting access token and updating the refresh token
    # If error occured, log it and exit
    authorize_to_d2l = d2l_functions.trade_in_refresh_token(config)
    if not authorize_to_d2l or 'access_token' not in authorize_to_d2l or 'refresh_token' not in authorize_to_d2l:
        logger.error('Failed to retrieve access or refresh token.')
        sys.exit(1)

    access_token = authorize_to_d2l['access_token']
    refresh_token = authorize_to_d2l['refresh_token']
    set_refresh_token(refresh_token)
    logger.info('Tokens are set.')

    # All grades reports parameters
    # Organizatiobal level, starting 4 year back and until today
    current_date = date.today()
    current_date_str = current_date.strftime("%Y-%m-%d")
    start_date_str = (current_date.replace(year=current_date.year - 4)).strftime("%Y-%m-%d")
    all_grades_params = {
        "DataSetId": config['dataset_id'],
        "Filters": [
            {
                "Name":"parentOrgUnitId",
                "Value":6606
            },
            {
                "Name":"startDate", 
                "Value": start_date_str
            },
            {
                "Name":"endDate", 
                "Value":current_date_str
            }
        ]
    }

    # Creating a request job for All Grades report with above parameters
    # If error occured, log it and exit
    request_report_url = f"{config['bspace_url']}{API_URL}create"
    request_all_grades = d2l_functions.post_with_auth(request_report_url, access_token, data=all_grades_params)

    if not request_all_grades:
        logger.error('Failed to rerequest all grades reports.')
        sys.exit(1)
    else:
        export_job_id = request_all_grades.json().get("ExportJobId")


    # Checking request job's status every 60 seconds
    # Will timeout and exit the script if 2 hours pass
    start_time = time.time()
    timeout = 2 * 60 * 60
    delay = 60

    while True:
        status_url = f"{config['bspace_url']}{API_URL}jobs/{export_job_id}"
        status_response = d2l_functions.get_with_auth(status_url, access_token, stream=False)
        if not status_response:
            logger.warning("Failed to get job status, retrying...")
        else:
            status = status_response.json().get("Status")
            if status == 2:
                logger.info("Export job completed successfully.")
                break
            elif status == 3:
                logger.error("Export job failed.")
                sys.exit(1)
            elif status == 4:
                logger.error("Export file was deleted before download.")
                sys.exit(1)
        if time.time() - start_time > timeout:
            logger.error("Job did not complete within the expected time.")
            sys.exit(1)
        time.sleep(delay)


    # Dwonloading the report
    download_url = f"{config['bspace_url']}{API_URL}download/{export_job_id}"
    download_response = d2l_functions.get_with_auth(download_url, access_token, stream=True)
    if download_response and download_response.status_code == 200:
        content_disposition = download_response.headers.get('Content-Disposition')
        if content_disposition and 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[-1].strip(' "')
            zip_path = os.path.join(base, filename)
            with open(zip_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Export file downloaded successfully: {zip_path}")
        else:
            logger.error("Failed to extract filename from response headers.")
            sys.exit(1)
    else:
        logger.error("Failed to download export file.")
        sys.exit(1)


    # Delete old 3 day old reports
    delete_x_days_old_reports(base)

# Main 
if __name__ == "__main__":
    main()
