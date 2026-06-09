"""Data Download Module

This module handles downloading raw Parquet files from NYC Open Data.
Features:
- Parallel downloads with configurable worker pool
- Retry logic with exponential backoff
- Idempotency (skip existing valid files)
- Streaming download for large files
- Comprehensive error handling and reporting
"""

import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

# Configuration
DOWNLOAD_TIMEOUT = 300  # 5 minutes per file
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_WORKERS = 4  # Parallel downloads


def download_parquet(base_url: str, raw_path: str, taxi_type: str, year: int, month: int) -> Dict:
    """Download raw Parquet file from NYC Open Data with retry and timeout.
    
    Args:
        base_url: Base URL for NYC Open Data
        raw_path: Destination base path (volume path)
        taxi_type: Type of taxi (yellow, green, fhv, fhvhv)
        year: Year to download
        month: Month to download
    
    Returns:
        dict with status, taxi_type, filename, path, size_mb, attempts
    """
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{base_url}/{filename}"
    dest_dir = f"{raw_path}/{taxi_type}"
    dest_path = f"{dest_dir}/{filename}"
    
    os.makedirs(dest_dir, exist_ok=True)
    
    # 1. Check if file already exists and is valid (idempotency)
    if os.path.exists(dest_path):
        size_mb = os.path.getsize(dest_path) / 1e6
        if size_mb > 0.1:  # Valid file (> 100KB)
            return {
                "status": "skipped",
                "taxi_type": taxi_type,
                "filename": filename,
                "path": dest_path,
                "size_mb": round(size_mb, 1),
                "attempts": 0
            }
    
    # 2. Download with retry
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()
            
            # Streaming download (for large files)
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Validate size
            size_mb = os.path.getsize(dest_path) / 1e6
            if size_mb < 0.01:  # < 10KB = corrupted
                raise ValueError(f"The data is too small: {size_mb:.2f} MB")
            
            return {
                "status": "success",
                "taxi_type": taxi_type,
                "filename": filename,
                "path": dest_path,
                "size_mb": round(size_mb, 1),
                "attempts": attempt
            }
            
        except Exception as e:
            if attempt < MAX_RETRIES:
                print(f"  Retry {attempt}/{MAX_RETRIES}: {filename} ({str(e)[:50]})")
                time.sleep(RETRY_DELAY * attempt)  # Exponential backoff
            else:
                return {
                    "status": "error",
                    "taxi_type": taxi_type,
                    "filename": filename,
                    "error": str(e),
                    "attempts": attempt
                }


def download_all_parallel(base_url: str, raw_path: str, taxi_types: List[str], 
                         years: List[int], months: List[int]) -> Dict:
    """Parallel download of all files with monitoring.
    
    Args:
        base_url: Base URL for NYC Open Data
        raw_path: Destination base path
        taxi_types: List of taxi types to download
        years: List of years to download
        months: List of months to download
    
    Returns:
        dict with download statistics
    """
    start_time = time.time()
    
    # Create task list
    tasks = [(t, y, m) for t in taxi_types for y in years for m in months]
    
    print(f"Starting parallel download: {len(tasks)} files, {MAX_WORKERS} workers")
    
    # Execute downloads in parallel
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_parquet, base_url, raw_path, t, y, m): (t, y, m) 
            for t, y, m in tasks
        }
        for future in as_completed(futures):
            results.append(future.result())
    
    # Calculate statistics
    duration = time.time() - start_time
    success = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "error")
    total_mb = sum(r.get("size_mb", 0) for r in results if r["status"] in ["success", "skipped"])
    
    stats = {
        "total_files": len(tasks),
        "downloaded": success,
        "skipped": skipped,
        "failed": failed,
        "total_mb": round(total_mb, 1),
        "duration_seconds": round(duration, 1),
        "results": results
    }
    
    # Summary
    print(f"\nDownload complete: {success} new, {skipped} skipped, {failed} failed")
    print(f"Total: {total_mb:.1f} MB in {duration:.1f}s")
    
    if failed > 0:
        print(f"\nWARNING: {failed} file(s) failed:")
        for r in [r for r in results if r["status"] == "error"]:
            print(f"  {r['filename']}: {r['error'][:60]}")
    
    return stats


if __name__ == "__main__":
    # Example usage
    from config import BASE_URL, RAW_PATH, TAXI_TYPES, YEARS, MONTHS
    stats = download_all_parallel(BASE_URL, RAW_PATH, TAXI_TYPES, YEARS, MONTHS)
