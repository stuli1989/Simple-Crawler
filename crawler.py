import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import threading
from queue import Queue
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import traceback
import time
from datetime import datetime, timedelta

# Constants for estimating time remaining
START_TIME = datetime.now()
URLS_CHECKED = 0
TOTAL_URLS = 0

# Utility function to clean and normalize URLs by removing query parameters and fragments
def clean_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

# Function to check the response code of a URL and categorize it as broken or not
def check_url(url):
    try:
        # Wait for the whole page to load before moving onto the next one (use GET request)
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 200:
            return 'OK', response.elapsed.total_seconds()
        else:
            return f"Broken: {response.status_code}", response.elapsed.total_seconds()
    except requests.exceptions.RequestException as e:
        return f"Broken: {e}", None
	
# Function to process sitemap index and sitemap files
def parse_sitemap(sitemap_url, seen_urls, lock, q):
    try:
        response = requests.get(sitemap_url)
        if response.status_code != 200:
            print(f"Error accessing sitemap at {sitemap_url}")
            return
        soup = BeautifulSoup(response.content, 'xml')
        if soup.find('sitemapindex'):
            # Handle sitemap index files
            sitemaps = [sm.find('loc').text for sm in soup.find_all('sitemap')]
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Pass the queue 'q' as an argument to each future call
                futures = [executor.submit(parse_sitemap, sitemap_loc, seen_urls, lock, q)
                           for sitemap_loc in sitemaps]
                for future in futures:
                    future.result()  # Wait for all submitted sitemaps to be processed
        elif soup.find('urlset'):
            # Handle regular sitemaps
            for url_tag in soup.find_all('url'):
                loc = url_tag.find('loc').text
                clean_loc = clean_url(loc)
                with lock:  # Use a context manager for locking
                    if clean_loc not in seen_urls:
                        seen_urls.add(clean_loc)
                        q.put(clean_loc)
                        # Assuming TOTAL_URLS is a global variable you're tracking
                        global TOTAL_URLS
                        TOTAL_URLS += 1
    except Exception as e:
        print(f"Exception parsing sitemap at {sitemap_url}: {e}")
        traceback.print_exc()

# Function to update progress
def update_progress():
    while not progress_update_event.is_set():
        time.sleep(5)  # Update every 5 seconds
        elapsed_time = datetime.now() - START_TIME
        urls_pending = TOTAL_URLS - URLS_CHECKED
        percent_complete = (URLS_CHECKED / TOTAL_URLS) * 100 if TOTAL_URLS else 0
        estimated_time_remaining = (elapsed_time / URLS_CHECKED * urls_pending) if URLS_CHECKED else timedelta(0)
        print(f"Checked URLs: {URLS_CHECKED}/{TOTAL_URLS} | Pending: {urls_pending} | "
              f"Complete: {percent_complete:.2f}% | Est. Time Remaining: {estimated_time_remaining}")

# Function to find additional URLs on each page
def fetch_and_process_url(q, seen_urls, lock, results, broken_urls):
    global URLS_CHECKED
    global TOTAL_URLS  # Declare TOTAL_URLS as global
    while True:
        url = q.get()
        status, elapsed = check_url(url)
        lock.acquire()
        try:
            URLS_CHECKED += 1
            if "Broken" in status:
                broken_urls.append((url, status, datetime.now(), elapsed))
            else:
                results.append((url, status, datetime.now(), elapsed))

                # Only proceed to fetch new links if the URL is OK
                if status == 'OK':
                    page_content = requests.get(url).text
                    soup = BeautifulSoup(page_content, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        new_url = clean_url(urljoin(url, link['href']))
                        if new_url not in seen_urls:
                            seen_urls.add(new_url)
                            q.put(new_url)
                            TOTAL_URLS += 1
        finally:
            lock.release()
        q.task_done()
		
# Global event for signaling the update thread to stop
progress_update_event = threading.Event()

# Main crawling function
def crawl_website(start_url):
    # Initialize queue, sets for seen URLs and lock
    q = Queue()
    seen_urls = set()
    lock = threading.Lock()
    
    # Lists to store results
    results = []  # List of tuples for successfully fetched URLs
    broken_urls = []  # List of tuples for broken URLs
    
    # Parse the sitemap and add the URLs to the queue
    parse_sitemap(urljoin(start_url, '/sitemap.xml'), seen_urls, lock, q)
    
    # Start threads for fetching URLs
    num_threads = 20  # Number of threads can be adjusted based on requirements
    for _ in range(num_threads):
        worker = threading.Thread(target=fetch_and_process_url,
                                  args=(q, seen_urls, lock, results, broken_urls))
        worker.daemon = True
        worker.start()
    
    # Start a separate thread for updating progress
    update_thread = threading.Thread(target=update_progress)
    update_thread.daemon = True
    update_thread.start()
    
    # Wait for the queue to be empty
    q.join()
    
    # Signal the update thread to stop and wait for it to finish
    progress_update_event.set()
    update_thread.join()
    
    # Output results to Excel files
    df_results = pd.DataFrame(results, columns=['URL', 'Status', 'Time Checked', 'Response Time'])
    df_broken = pd.DataFrame(broken_urls, columns=['Broken URL', 'Error Status', 'Time Checked', 'Response Time'])
    df_results.to_excel('all_urls_checked.xlsx', index=False)
    df_broken.to_excel('broken_urls.xlsx', index=False)

    print("Crawling has finished.")

# Function to start the crawler with user input
def start_crawler():
    start_url = input("Enter the main URL of the eCommerce website: ")
    crawl_website(start_url)
	
import logging

# Setup logging configuration
logging.basicConfig(filename='crawler_errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Modify the exception handling in the main entry point to log errors
if __name__ == "__main__":
    try:
        start_crawler()
    except Exception as e:
        logging.error("An error occurred during the crawling process", exc_info=True)
        print(f"An error occurred during the crawling process: {e}")
        traceback.print_exc()
    finally:
        input("Crawling has completed or an error has occurred. Press Enter to close the program...")
