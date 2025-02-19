from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# MongoDB connection string and client initialization
uri_collection_one = "mongodb://localhost:27017/"
client_collection_one = MongoClient(uri_collection_one)

# Database and collection initialization
animal_food_db = client_collection_one['chfa_directory']
unmatch_india_collection = animal_food_db['Not_avail_domains']

# Headers for HTTP requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Serper API Key
SERPER_API_KEY = "b675913072e8fc94e598d86c445282f78fec5f0d"

def search_with_serper(name):
    url = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }
    query = {
        "q": name
    }

    try:
        response = requests.post(url, json=query, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error while querying Serper API for {name}: {e}")
        return None

def extract_website_from_serper_result(result):
    if result and "organic" in result:
        for item in result["organic"]:
            if "link" in item and check_url_path(item["link"]):
                return item["link"]
    return None

def get_linkedin_website_info(linkedin_url):
    website = None
    try:
        response = requests.get(linkedin_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        data_container = soup.find('div', class_='core-section-container__content break-words')
        website_link = data_container.find('a', class_='link-no-visited-state')
        if website_link:
            if "Get directions" not in website_link.text.strip():
                website = website_link.text.strip()
    except Exception as e:
        print(f"Error while scraping LinkedIn: {e}")
    return website

def check_url_path(url):
    # Ensure the URL starts with 'http://' or 'https://'
    if not re.match(r'http[s]?://', url):
        url = 'http://' + url

    # Match the URL and extract the path
    match = re.match(r'http[s]?://[^/]+(/.*)?', url)

    if match:
        path = match.group(1) if match.group(1) else ''
    else:
        return False

    # List of valid paths to allow
    valid_paths = ['/', '/en', '/en/', '/en.php', '/en.html']

    # Check if the path is empty (root domain) or matches valid paths
    if path == '' or path in valid_paths:
        return True

    # Additional check for variations of /en/ paths
    if re.match(r'^/[a-z]{2}/?$', path):  # Matches /en/, /fr/, /de/, etc.
        return True

    return False

def process_company(document):
    name = document.get('name')
    if name:
        url_found = False
        website_info = None

        try:
            # First, search the company's LinkedIn page and attempt to scrape the website from it
            serper_result = search_with_serper(name)

            # Extract a valid LinkedIn URL or company website
            if serper_result:
                for result in serper_result.get('organic', []):
                    if 'linkedin.com' in result.get('link', ''):
                        website_info = get_linkedin_website_info(result['link'])
                        if website_info and check_url_path(website_info):
                            url_found = True
                            break

            # If LinkedIn didn't work, search for the company website directly using Serper
            if not url_found:
                website_info = extract_website_from_serper_result(serper_result)
                if website_info:
                    url_found = True

            # Update MongoDB document
            if url_found:
                unmatch_india_collection.update_one(
                    {"_id": document["_id"]},
                    {"$set": {"url": website_info}}
                )
                print(f"Updated {name} with URL: {website_info}")
            else:
                unmatch_india_collection.update_one(
                    {"_id": document["_id"]},
                    {"$set": {"url": None}}
                )
                print(f"Could not find a valid website for '{name}', set URL to null.")

        except Exception as e:
            print(f"Error processing company {name}: {e}")
            unmatch_india_collection.update_one(
                {"_id": document["_id"]},
                {"$set": {"url": None}}
            )

def update_company_urls_with_multithreading(max_threads=50):
    # Get all documents that have a company name
    documents = list(unmatch_india_collection.find(
        {"name": {"$exists": True}},
        {"name": 1}
    ))

    # Use ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit tasks to the thread pool
        futures = [executor.submit(process_company, doc) for doc in documents]

        # Process results as they are completed
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread: {e}")

if __name__ == "__main__":
    try:
        update_company_urls_with_multithreading(max_threads=50)
    except Exception as e:
        print(f"Main error: {e}")
