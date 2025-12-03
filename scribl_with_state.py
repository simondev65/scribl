#!/usr/bin/python3

# Improved Scribl Script
# Features: Restartability (State file), Detailed Logging, Threading, Exporttool via CLI
# Usage: Run directly on the Splunk Indexer.

import argparse
import os
import subprocess
import sys
import time
import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
STATE_FILE = "scribl_state.json"
SPLUNK_BIN = "/opt/splunk/bin/splunk" # Adjust if your splunk binary is elsewhere

# Thread-safe lock for writing to the state file
state_lock = threading.Lock()

# --- HELPER FUNCTIONS ---

def setup_logging(log_file):
    """Sets up logging to file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_state():
    """Loads the list of fully processed bucket paths."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_buckets', []))
        except Exception as e:
            logging.error(f"Failed to load state file {STATE_FILE}: {e}")
            return set()
    return set()

def save_state(processed_buckets):
    """Saves the set of processed buckets to disk."""
    with state_lock:
        try:
            with open(STATE_FILE, 'w') as f:
                # Convert set to list for JSON serialization
                json.dump({'processed_buckets': list(processed_buckets)}, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save state: {e}")

def get_bucket_list(directory, earliest, latest):
    """Scans the directory for buckets matching the time range."""
    valid_buckets = []
    
    if not os.path.exists(directory):
        logging.error(f"Directory not found: {directory}")
        return []

    # List all directories
    try:
        dirs = os.listdir(directory)
    except OSError as e:
        logging.error(f"Error listing directory {directory}: {e}")
        return []

    for dir_name in dirs:
        # Splunk buckets usually start with db_ or rb_ (replicated)
        # Naming convention: db_<newest_epoch>_<oldest_epoch>_<id>
        if dir_name.startswith("db_") or dir_name.startswith("rb_"):
            full_path = os.path.join(directory, dir_name)
            
            try:
                parts = dir_name.split('_')
                # We expect at least 3 parts: db, max_epoch, min_epoch
                if len(parts) < 3:
                    continue
                
                max_epoch = int(parts[1])
                min_epoch = int(parts[2])

                # Check overlap: 
                # Bucket is valid if its range overlaps with requested [earliest, latest]
                # Logic: Not (BucketEnd < RequestStart OR BucketStart > RequestEnd)
                if not (max_epoch < earliest or min_epoch > latest):
                    valid_buckets.append(full_path)
            
            except ValueError:
                # Could not parse integer timestamps, skip this folder
                logging.warning(f"Skipping folder with invalid format: {dir_name}")
                continue

    return valid_buckets

def process_bucket(bucket_path, args, processed_buckets):
    """
    Worker function: Runs exporttool piped to netcat for a single bucket.
    """
    # 1. Check if already done (Double check inside thread for safety)
    if bucket_path in processed_buckets:
        return

    logging.info(f"STARTING bucket: {bucket_path}")
    start_time = time.time()

    # 2. Build the command
    # /opt/splunk/bin/splunk cmd exporttool /path/to/bucket /dev/stdout -csv | nc <host> <port>
    
    export_cmd = f"{SPLUNK_BIN} cmd exporttool '{bucket_path}' /dev/stdout -csv"
    
    nc_options = ""
    if args.TLS:
        nc_options = "--ssl"
    
    # We use a shell pipe to connect exporttool to nc. 
    # This is efficient as data doesn't pass through Python memory.
    full_command = f"{export_cmd} | nc {nc_options} {args.remoteIP} {args.remotePort}"

    try:
        # 3. Execute Command
        # shell=True is required for the pipe (|) to work
        process = subprocess.run(
            full_command, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            executable="/bin/bash" # Use bash to potentially support pipefail if needed
        )

        # 4. Handle Result
        if process.returncode == 0:
            elapsed = time.time() - start_time
            logging.info(f"FINISHED bucket: {bucket_path}. Time taken: {elapsed:.2f}s")
            
            # Update State
            with state_lock:
                processed_buckets.add(bucket_path)
            save_state(processed_buckets)
        else:
            # Capture stderr for debugging
            error_msg = process.stderr.decode('utf-8', errors='ignore').strip()
            logging.error(f"FAILED bucket: {bucket_path}. Exit Code: {process.returncode}. Error: {error_msg}")

    except Exception as e:
        logging.error(f"CRITICAL ERROR on bucket {bucket_path}: {e}")

# --- MAIN ---

def getArgs():
    parser = argparse.ArgumentParser(description="Export Splunk buckets via exporttool and stream to Netcat (Cribl).")
    
    parser.add_argument("-d","--directory", help="Source directory containing the buckets (e.g., /opt/splunk/var/lib/splunk/defaultdb/db)", required=True)
    parser.add_argument("-r","--remoteIP", help="Remote address to send the exported data to", required=True)
    parser.add_argument("-p","--remotePort", help="Remote TCP port to be used", required=True)
    
    parser.add_argument("-t","--TLS", help="Send with TLS enabled (nc --ssl)", action='store_true')
    parser.add_argument("-n","--numstreams", default=4, type=int, help="Number of parallel streams (threads)")
    parser.add_argument("-l","--logfile", default="/tmp/scribl.log", help="Log file location")
    
    parser.add_argument("-et","--earliest", default=0, type=int, help="Earliest epoch time for bucket selection")
    parser.add_argument("-lt","--latest", default=9999999999, type=int, help="Latest epoch time for bucket selection")
    
    return parser.parse_args()

def main():
    args = getArgs()
    setup_logging(args.logfile)
    
    logging.info("--- Script Startup ---")
    logging.info(f"Configuration: Directory={args.directory}, Target={args.remoteIP}:{args.remotePort}, Threads={args.numstreams}")

    # 1. Load State
    processed_buckets = load_state()
    logging.info(f"Resuming with {len(processed_buckets)} buckets already marked as completed in {STATE_FILE}.")

    # 2. Find Buckets
    logging.info("Scanning directory for buckets...")
    all_buckets = get_bucket_list(args.directory, args.earliest, args.latest)
    
    # 3. Filter Work Queue
    work_queue = [b for b in all_buckets if b not in processed_buckets]
    
    logging.info(f"Total buckets found matching time range: {len(all_buckets)}")
    logging.info(f"Buckets remaining to process: {len(work_queue)}")

    if not work_queue:
        logging.info("No buckets to process. Exiting.")
        sys.exit(0)

    # 4. Start Thread Pool
    # We use ThreadPoolExecutor. Since the heavy lifting is done by the subprocess (exporttool),
    # Python threads are sufficient to manage the orchestration without blocking.
    logging.info(f"Starting execution with {args.numstreams} threads...")
    
    with ThreadPoolExecutor(max_workers=args.numstreams) as executor:
        # Submit all tasks
        futures = [
            executor.submit(process_bucket, bucket, args, processed_buckets)
            for bucket in work_queue
        ]
        
        # We don't strictly need to wait for futures here as the 'with' block waits,
        # but you could add logic here to track overall progress percentage.

    logging.info("All tasks completed.")
    logging.info("--- Script Shutdown ---")

if __name__ == "__main__":
    main()