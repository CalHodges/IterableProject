import os
import sqlite3
import requests
import logging
import csv
import json
import time
import jwt
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv
from time import sleep

load_dotenv()
API_KEY = os.getenv("ITERABLE_API_KEY")
JWT_SECRET = os.getenv("JWT_SECRET")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("iterable_sync.log"),
        logging.StreamHandler()
    ]
)

DB_PATH = "Data/customer_data.db"  
SQL_PATH = "Scripts/recent_views_query.sql"  
ITERABLE_BASE = "https://api.iterable.com"

def generate_jwt() -> str:
    """
    Generate a JWT token using the JWT_SECRET.
    """
    payload = {"iat": int(time.time())}
    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return token

def load_query(sql_path: str) -> str:
    with open(sql_path, 'r') as f:
        return f.read()

def query_db(plan_type: str) -> List[Dict]:
    sql = load_query(SQL_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(sql, (plan_type,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        conn.close()
    return [dict(zip(cols, row)) for row in rows]

def request_with_retry(
    method: str, 
    url: str, 
    json_payload: Dict[str, Any], 
    headers: Dict[str, str], 
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Make HTTP request with exponential backoff retry logic.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: Target URL
        json_payload: JSON data to send
        headers: HTTP headers
        max_retries: Maximum number of retry attempts
        
    Returns:
        JSON response as dictionary
        
    Raises:
        requests.RequestException: If all retry attempts fail
    """
    backoff = 1
    last_exception: Optional[Exception] = None
    
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method, url, json=json_payload, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
            
        except requests.HTTPError as e:
            last_exception = e
            logging.error(f"HTTP Error {resp.status_code}: {resp.text}")
            if attempt < max_retries:
                logging.warning(f"Attempt {attempt} failed: {e}. Retrying in {backoff}s...")
                sleep(backoff)
                backoff *= 2
            else:
                logging.error(f"All {max_retries} attempts failed for {url}: {e}")
                raise
                
        except requests.RequestException as e:
            last_exception = e
            if attempt < max_retries:
                logging.warning(f"Request failed: {e}. Retrying in {backoff}s...")
                sleep(backoff)
                backoff *= 2
            else:
                logging.error(f"All {max_retries} attempts failed for {url}: {e}")
                raise
    
    # This should never be reached, but satisfies type checkers
    if last_exception:
        raise last_exception
    raise requests.RequestException("Unexpected error in retry logic")

def send_user_update(user: Dict) -> Dict:
    """
    Send user profile update to Iterable API.
    """
    url = f"{ITERABLE_BASE}/api/users/update"
    headers = {
        "Authorization": f"Bearer {generate_jwt()}",
        "Content-Type": "application/json",
        "Api-Key": API_KEY
    }
    payload = {
        "email": user["email"],
        "userId": str(user["user_id"]),  # Ensure string format
        "dataFields": {
            "first_name": user["first_name"],
            "last_name": user["last_name"],
            "plan_type": user["plan_type"],
            "recent_page_view": True,
            "candidate": user["candidate"]
        }
    }
    return request_with_retry("POST", url, payload, headers)

def send_event_track(user: Dict) -> Dict:
    """
    Send page view event to Iterable API.
    """
    url = f"{ITERABLE_BASE}/api/events/track"
    headers = {
        "Authorization": f"Bearer {generate_jwt()}",
        "Content-Type": "application/json",
        "Api-Key": API_KEY
    }
    payload = {
        "email": user["email"],
        "userId": str(user["user_id"]),  # Ensure string format
        "eventName": "page_view",
        "dataFields": {
            "page": user["page"],
            "browser": user["browser"],
            "location": user["location"],
            "timestamp": user["event_time"],
            "candidate": user["candidate"]
        }
    }
    return request_with_retry("POST", url, payload, headers)

def export_to_csv(filename: str, fieldnames: List[str], rows: List[Dict]):
    """
    Export data to CSV file.
    """
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def main():
    """
    Main execution flow.
    """
    logging.info("Starting Iterable sync process...")
    
    if not API_KEY or not JWT_SECRET:
        logging.error("Missing required environment variables. Please check your .env file.")
        return
    
    plan = "pro"
    records = query_db(plan)
    logging.info(f"Found {len(records)} '{plan}' users with recent pricing/settings views.")
    
    if not records:
        logging.info("No records found. Exiting.")
        return

    # Bonus: export raw SQL results
    export_to_csv("recent_views.csv", list(records[0].keys()), records)
    logging.info("Exported recent_views.csv")

    api_results = []
    for user in records:
        result = {"user_id": user["user_id"], "email": user["email"]}
        
        try:
            upd_resp = send_user_update(user)
            logging.info(f"User update success for {user['email']}")
            result["update_response"] = json.dumps(upd_resp)
        except Exception as e:
            logging.error(f"User update failed for {user['email']}: {e}")
            result["update_response"] = f"ERROR: {str(e)}"

        sleep(1) 

        try:
            track_resp = send_event_track(user)
            logging.info(f"Event track success for {user['email']}")
            result["track_response"] = json.dumps(track_resp)
        except Exception as e:
            logging.error(f"Event track failed for {user['email']}: {e}")
            result["track_response"] = f"ERROR: {str(e)}"

        api_results.append(result)
        sleep(1)  

    export_to_csv("api_results.csv", ["user_id", "email", "update_response", "track_response"], api_results)
    logging.info("Exported api_results.csv")
    logging.info("Iterable sync process completed.")

if __name__ == "__main__":
    main()