import os
from dotenv import load_dotenv
import json
import logging
import time
from datetime import datetime, timezone
import requests
import pandas as pd
from databricks.sdk import WorkspaceClient
load_dotenv()
# Logging Configuration
log_filename = "pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] DataOpsPipeline - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OpenSkyExtractor")

# Configuration & Bounding Box
OPENSKY_URL = "https://opensky-network.org/api/states/all"
OUTPUT_DIR = "data/raw"

BOUNDS = {
    "lamin": 45.0,
    "lamax": 55.0,
    "lomin": 5.0,
    "lomax": 15.0
}

OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", 
    "last_contact", "longitude", "latitude", "baro_altitude", 
    "on_ground", "velocity", "true_track", "vertical_rate", 
    "sensors", "geo_altitude", "squawk", "spi", "position_source"
]

# Extraction logic with retries

def fetch_opensky_states(max_retries=3, backoff_factor=10):
    params = BOUNDS
    headers = {"User-Agent": "DataOpsPortfolioPipeline/1.0"}

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Initiating API request to opensky Attempt {attempt}/{max_retries} ...")
            start_time = time.time()

            response = requests.get(OPENSKY_URL, params=params, headers=headers, timeout=15)
            latency = round(time.time() - start_time, 2)

            if response.status_code == 429:
                wait_time = backoff_factor * attempt
                logger.warning(f"Rate limited (429). Waiting {wait_time}s before retrying..")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            payload = response.json()

            logger.info(f"API Request Successful | Status: 200 | Latency: {latency}s")
            return payload, latency
        except requests.exceptions.Timeout:
            logger.error(f"Attempt {attempt}: Request timed out.")
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Attempt {attempt}: HTTP error occurred - {http_err}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Attempt {attempt}: Network error - {req_err}")

        time.sleep(backoff_factor)

    logger.critical("Retries limit reached. Pipeline extraction failed.")
    return None, None

def process_and_save_raw_payload(payload, latency):
    if not payload or "states" not in payload or payload["states"] is None:
        logger.warning("Received empty or invalid state payload.")
        return None  # Return None on failure
    
    states_data = payload["states"]
    record_count = len(states_data)
    extraction_timestamp = datetime.now(timezone.utc)
    timestamp_str = extraction_timestamp.strftime("%Y%m%d_%H%M%S")
    logger.info(f"Extracted {record_count} state vectors from API.")

    df = pd.DataFrame(states_data, columns=OPENSKY_COLUMNS)

    df["ingestion_timestamp_utc"] = extraction_timestamp.isoformat()
    df["api_latency_seconds"] = latency
    df["batch_id"] = f"BATCH_{timestamp_str}"

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_name = f"opensky_raw_{timestamp_str}.json"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    df.to_json(file_path, orient="records", lines=True)

    logger.info(f"Batch successfully saved to disk: {file_path}")
    logger.info(f"Pipeline Audit: Records = {record_count} | Output Size = {os.path.getsize(file_path)} bytes")
    
    # RETURN THE FILE PATH STRING (NOT TRUE)
    return file_path
def upload_to_databricks_volume(local_file_path):
    """Uploads local raw JSON file to Databricks Volume using workspace API credentials."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not host or not token:
        logger.error("Databricks environment variables missing. Skipping cloud upload.")
        return

    w = WorkspaceClient(host=host, token=token)
    volume_path = f"/Volumes/skynet/default/raw_open_sky_landing/{os.path.basename(local_file_path)}"
    
    with open(local_file_path, "rb") as f:
        w.files.upload(volume_path, f, overwrite=True)
    
    logger.info(f"Successfully pushed batch to Databricks Volume: {volume_path}")

if __name__ == "__main__":
    logger.info(" Starting OpenSky Phase 1 Extraction Pipeline ")
    
    raw_payload, api_latency = fetch_opensky_states()
    
    if raw_payload:
        saved_file_path = process_and_save_raw_payload(raw_payload, api_latency)
        
        # If saved_file_path is a string path (not None), execute upload
        if saved_file_path:
            upload_to_databricks_volume(saved_file_path)
            logger.info(" Phase 1 Pipeline & Databricks Sync Completed Successfully ")
        else:
            logger.error(" Phase 1 Pipeline Completed with Warnings (No Data Saved) ")
    else:
        logger.error(" Phase 1 Pipeline Failed ")

