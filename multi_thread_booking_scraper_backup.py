import re
import time
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from collections import defaultdict
from selenium.webdriver.common.action_chains import ActionChains
import requests
from typing import Optional
from datetime import date, timedelta
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import logging

# Set up logging for thread safety
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Thread-%(thread)d - %(message)s')
logger = logging.getLogger(__name__)


# Thread-safe CSV writer
class ThreadSafeCSVWriter:
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.Lock()

    def write_batch(self, properties_data, is_first_batch=False):
        """Thread-safe version of save_batch_to_csv"""
        if not properties_data:
            return

        # Get all possible fieldnames from all properties in this batch
        all_fieldnames = set()
        for property_data in properties_data:
            all_fieldnames.update(property_data.keys())

        # If not first batch, we need to get existing fieldnames from the file
        existing_fieldnames = set()
        if not is_first_batch:
            try:
                with open(self.filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    existing_fieldnames = set(reader.fieldnames or [])
            except FileNotFoundError:
                # File doesn't exist, treat as first batch
                is_first_batch = True

        # Combine all fieldnames
        fieldnames = sorted(list(all_fieldnames.union(existing_fieldnames)))

        with self.lock:
            # Check if file exists after acquiring lock
            try:
                with open(self.filename, 'r') as f:
                    file_exists = True
            except FileNotFoundError:
                file_exists = False

            # Write mode: 'w' for first batch, 'a' for subsequent batches
            mode = 'w' if not file_exists else 'a'

            with open(self.filename, mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header only if file doesn't exist
                if not file_exists:
                    writer.writeheader()

                # Write all properties in this batch
                for property_data in properties_data:
                    # Fill missing fields with default values
                    csv_row = {}
                    for field in fieldnames:
                        if field in property_data:
                            csv_row[field] = property_data[field]
                        else:
                            # Default values based on field type
                            if field == 'property_url':
                                csv_row[field] = ''
                            elif field in ['category', 'address', 'zone', 'city', 'WiFi Speed']:
                                csv_row[field] = None
                            else:
                                csv_row[field] = 0
                    writer.writerow(csv_row)

        logger.info(f"✅ Saved batch of {len(properties_data)} properties to {self.filename}")


def url_builder(destinations: list):
    """
    Builds a list of Booking.com URLs for multiple destinations with specific parameters.
    """
    base_url = "https://www.booking.com/searchresults.html?"
    today = date.today()
    tomorrow = today + timedelta(days=1)
    checkin_date = today.strftime("%Y-%m-%d")
    checkout_date = tomorrow.strftime("%Y-%m-%d")

    urls = []
    for city in destinations:
        params = {
            'ss': city,
            'checkin': checkin_date,
            'checkout': checkout_date,
            'group_adults': 1,
            'no_rooms': 1,
            'group_children': 0,
        }

        url_parts = [f"{key}={value}" for key, value in params.items()]
        full_url = base_url + "&".join(url_parts)
        urls.append(full_url)

    return urls


def scrape_booking_results(urls: list):
    """
    Scrapes the URLs of all property offers from Booking.com search results.
    """
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    all_property_urls = {}

    try:
        for search_url in urls:
            print(f"--- Navigating to: {search_url} ---")
            driver.get(search_url)

            # Handle cookie consent
            try:
                accept_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                accept_button.click()
                print("Clicked 'Accept' on cookie pop-up.")
            except TimeoutException:
                print("No cookie pop-up found or it didn't appear in time.")

            # Load all results by clicking "Show more results"
            while True:
                print("Scrolling down to load more results...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)

                try:
                    more_results_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, ".//span[contains(text(), ' more results')]"))
                    )
                    more_results_button.click()
                    print("Clicked 'Show more results'. Waiting for content to load...")
                    time.sleep(5)
                except (NoSuchElementException, TimeoutException):
                    print("No 'more results' button found. All listings should be loaded.")
                    break
                except Exception as e:
                    print(f"An unexpected error occurred while clicking the button: {e}")
                    break

            # Extract property URLs
            property_links = driver.find_elements(By.XPATH, '//a[@data-testid="title-link"]')
            scraped_urls = []

            if property_links:
                for link in property_links:
                    href = link.get_attribute('href')
                    if href:
                        scraped_urls.append(href)
                print(f"Scraped {len(scraped_urls)} property URLs for this search.")
                all_property_urls[search_url] = scraped_urls
            else:
                print("No property links found on the page.")
                all_property_urls[search_url] = []

    except Exception as e:
        print(f"An error occurred during the scraping process: {e}")
    finally:
        driver.quit()
        print("WebDriver closed.")

    return all_property_urls


def get_location_details(lat, lon, driver=None):
    """
    Get complete location details from coordinates in one API call.
    """
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&accept-language=en"
        headers = {'User-Agent': 'BookingScraper/1.0'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        address_components = data.get('address', {})
        latin_pattern = re.compile(r'[^a-zA-Z0-9\s\-,\.\']')
        address = latin_pattern.sub('', data.get('display_name', '')).strip()

        # Replace commas with tabs to avoid CSV confusion
        if address:
            address = address.replace(',', '\t')

        # Determine zone name
        zone = None
        zone_fields = [
            'neighbourhood', 'suburb', 'quarter', 'city_district',
            'residential', 'hamlet', 'village', 'town_district',
            'subdistrict', 'district', 'municipality', 'county'
        ]

        for field in zone_fields:
            if field in address_components and address_components[field]:
                raw_zone = address_components[field].strip()
                zone = latin_pattern.sub('', raw_zone).strip()
                if zone:
                    break

        # Determine city name
        city = None
        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="wrap-hotelpage-top"]/div[3]/div/div/div/div/div/span[1]/button/div'))
            )
            full_text = element.text
            city_match = re.search(r"\b\d{5}\s+([A-Za-z]+)\b", full_text)
            if city_match:
                city = city_match.group(1)
        except Exception as e:
            print(f"Error extracting city from Booking.com address: {e}")
            city_fields = ['city', 'town', 'municipality', 'village']
            for field in city_fields:
                if field in address_components and address_components[field]:
                    city = address_components[field].strip()
                    city = city.replace(' Prefecture', '').replace(' Province', '')
                    break

        return {
            'address': address if address else None,
            'zone': zone,
            'city': city
        }

    except Exception as e:
        print(f"Error getting location details: {e}")
        return {'address': None, 'zone': None, 'city': None}


def extract_category(driver) -> Optional[str]:
    """
    Extract property category with minimal normalization.
    """
    try:
        outer_span = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="breadcrumb-current"]'))
        )
        inner_span = outer_span.find_element(By.TAG_NAME, "span")
        full_text = inner_span.text.strip()

        match = re.search(r'\(([^)]+)\)', full_text)
        if match:
            category = match.group(1)
        else:
            category = full_text

        if category == 'Guest House':
            return 'Riad'
        elif category == 'Condo Hotel':
            return 'Apartment-Hotel'
        else:
            return category

    except Exception as e:
        print(f"Error extracting category: {e}")
        return None


def scrape_single_property_data(driver, property_url, thread_id=None):
    """
    Scrapes detailed data for a single property URL using the improved logic from single-threaded version.
    Returns a dictionary with all the scraped data.
    """
    thread_info = f"Thread {thread_id}: " if thread_id else ""
    print(f"\n{thread_info}=== Scraping property: {property_url} ===")

    # Initialize data structure
    property_data = {
        'property_url': property_url,
        'category': None,
        'general_review': 0,
        'general_review_count': 0,
        'comfort_score': 0,
        'value_of_money_score': 0,
        'location_score': 0,
        'free_wifi_score': 0,
        'avg_review_score_all': 0,
        'avg_review_score_all_count': 0,
        'avg_review_score_families': 0,
        'avg_review_score_families_count': 0,
        'avg_review_score_couples': 0,
        'avg_review_score_couples_count': 0,
        'avg_review_score_solo_travelers': 0,
        'avg_review_score_solo_travelers_count': 0,
        'avg_review_score_business_travellers': 0,
        'avg_review_score_business_travellers_count': 0,
        'min_price_per_night': 0,
        'max_price_per_night': 0,
        'latitude': None,
        'longitude': None,
        'address': None,
        'zone': None,
        'city': None,
        'WiFi Speed': None,
    }

    try:
        driver.get(property_url)
        time.sleep(3)

        # Extract property category
        try:
            property_data['category'] = extract_category(driver)
            print(f"{thread_info}Property category: {property_data['category']}")
        except Exception as e:
            print(f"{thread_info}Error extracting category: {e}")

        # SCRAPE PRICES
        try:
            print(f"{thread_info}Scraping prices...")
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'td.hprt-table-cell-price'))
            )

            price_elements = driver.find_elements(
                By.CSS_SELECTOR,
                'td.hprt-table-cell-price div.hprt-price-block div.prco-wrapper span.prco-valign-middle-helper'
            )

            prices = []
            for element in price_elements:
                price_text = element.text.strip()
                try:
                    price_numeric = ''.join(filter(str.isdigit, price_text))
                    if price_numeric:
                        price = int(price_numeric)
                        prices.append(price)
                except ValueError:
                    continue

            if prices:
                property_data['min_price_per_night'] = min(prices)
                property_data['max_price_per_night'] = max(prices)
                print(
                    f"{thread_info}Price range: {property_data['min_price_per_night']} - {property_data['max_price_per_night']} MAD")

        except Exception as e:
            print(f"{thread_info}Error scraping prices: {e}")

        # SCRAPE WIFI SPEED
        try:
            print(f"{thread_info}Scraping WiFi speed...")
            speed_element = driver.find_element(By.XPATH, "//div[contains(text(), 'Mbps')]")
            speed_text = speed_element.text.split('•')[-1].strip()
            property_data['WiFi Speed'] = speed_text
            print(f"{thread_info}WiFi Speed: {speed_text}")
        except Exception as e:
            property_data['WiFi Speed'] = 'Not specified'
            print(f"{thread_info}WiFi speed not found")

        # SCRAPE REVIEWS - Using improved logic from single-threaded version
        try:
            print(f"{thread_info}Scraping reviews...")

            # Click reviews button
            review_button_clicked = False
            try:
                review_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "js--hp-gallery-scorecard"))
                )
                review_btn.click()
                review_button_clicked = True
                print(f"{thread_info}Reviews button clicked")
            except Exception:
                try:
                    review_btn = driver.find_element(By.ID, "js--hp-gallery-scorecard")
                    driver.execute_script("arguments[0].click();", review_btn)
                    review_button_clicked = True
                    print(f"{thread_info}Reviews button clicked (JavaScript)")
                except Exception as e:
                    print(f"{thread_info}Could not click reviews button: {e}")

            if review_button_clicked:
                time.sleep(3)

                # Get review scores using the single-threaded version's logic
                try:
                    comfort_score = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[4]')
                        )
                    )
                    property_data['comfort_score'] = float(comfort_score.get_attribute("textContent"))
                    print(f"{thread_info}Comfort: {property_data['comfort_score']}")
                except Exception as e:
                    print(f"{thread_info}Error getting comfort score: {e}")

                try:
                    value_of_money_score = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[5]')
                        )
                    )
                    property_data['value_of_money_score'] = float(value_of_money_score.get_attribute("textContent"))
                    print(f"{thread_info}Value of money: {property_data['value_of_money_score']}")
                except Exception as e:
                    print(f"{thread_info}Error getting value of money score: {e}")

                try:
                    location_score = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[6]')
                        )
                    )
                    property_data['location_score'] = float(location_score.get_attribute("textContent"))
                    print(f"{thread_info}Location: {property_data['location_score']}")
                except Exception as e:
                    print(f"{thread_info}Error getting location score: {e}")

                try:
                    free_wifi_score = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[7]')
                        )
                    )
                    property_data['free_wifi_score'] = float(free_wifi_score.get_attribute("textContent"))
                    print(f"{thread_info}Free WiFi: {property_data['free_wifi_score']}")
                except Exception:
                    print(f"{thread_info}Free WiFi score not available")

                try:
                    general_score = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]')
                        )
                    )
                    property_data['general_review'] = float(general_score.get_attribute("textContent"))
                    print(f"{thread_info}General score: {property_data['general_review']}")
                except Exception as e:
                    print(f"{thread_info}Error getting general score: {e}")

                try:
                    reviews_count_element = driver.find_element(By.XPATH,
                                                                '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[4]/div[2]')
                    reviews_count_text = reviews_count_element.text.strip()
                    digits_only = ''.join(filter(str.isdigit, reviews_count_text))
                    if digits_only:
                        property_data['general_review_count'] = int(digits_only)
                        print(f"{thread_info}Reviews count: {property_data['general_review_count']}")
                except Exception as e:
                    print(f"{thread_info}Error getting reviews count: {e}")

                # Process reviews by traveler type - EXACT LOGIC from single-threaded version
                def process_reviews_for_category(category_value, category_name):
                    try:
                        select = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="customerType"]'))
                        )
                        select_element = Select(select)
                        select_element.select_by_value(category_value)
                        print(f"{thread_info}Selected '{category_name}' customer type")
                        time.sleep(2)

                        scores = []
                        traveler_types = defaultdict(list)

                        # Pagination loop - process ALL pages
                        page_count = 0
                        while True:
                            page_count += 1
                            print(f"{thread_info}Processing {category_name} page {page_count}")

                            try:
                                WebDriverWait(driver, 10).until(
                                    EC.presence_of_all_elements_located(
                                        (By.CSS_SELECTOR, '[data-testid="review-card"]'))
                                )
                                review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-testid="review-card"]')
                                print(f"{thread_info}Found {len(review_cards)} review cards on page {page_count}")

                                # Process reviews on current page
                                for i, card in enumerate(review_cards):
                                    try:
                                        # Extract score
                                        score_element = \
                                            card.find_element(By.XPATH,
                                                              './/div[contains(text(), "Scored")]').text.split(
                                                "Scored ")[1].strip()
                                        score = float(score_element)
                                        scores.append(score)

                                        # Extract traveler type (only for ALL category)
                                        if category_value == "ALL":
                                            traveler_type = "Unknown"
                                            try:
                                                traveler_element = card.find_element(
                                                    By.CSS_SELECTOR, '[data-testid="review-traveler-type"]'
                                                )
                                                traveler_type = traveler_element.text.strip() or "Unknown"
                                            except:
                                                pass
                                            traveler_types[traveler_type].append(score)

                                    except Exception as e:
                                        print(f"{thread_info}Error extracting review data from card {i + 1}: {str(e)}")

                                # Check for next page
                                try:
                                    next_btn = WebDriverWait(driver, 3).until(
                                        EC.element_to_be_clickable(
                                            (By.XPATH,
                                             '//*[@id="reviewCardsSection"]/div[2]/div[1]/div/div/div[3]/button')
                                        )
                                    )

                                    if "disabled" in next_btn.get_attribute("class"):
                                        print(f"{thread_info}Next button is disabled, reached last page")
                                        break

                                    next_btn.click()
                                    time.sleep(1.5)
                                    print(f"{thread_info}Clicked next page button")
                                except:
                                    print(f"{thread_info}No next button found, reached last page")
                                    break

                            except Exception as e:
                                print(f"{thread_info}Error processing page {page_count}: {e}")
                                break

                        print(f"{thread_info}Total scores collected for {category_name}: {len(scores)}")
                        return scores, traveler_types

                    except Exception as e:
                        print(f"{thread_info}Error in process_reviews_for_category for {category_name}: {e}")
                        return [], {}

                # Process ALL reviews to get traveler type breakdown
                print(f"{thread_info}Processing ALL reviews with full pagination...")
                all_scores, traveler_scores = process_reviews_for_category("ALL", "ALL")

                if all_scores:
                    property_data['avg_review_score_all'] = sum(all_scores) / len(all_scores)
                    property_data['avg_review_score_all_count'] = len(all_scores)

                print(f"{thread_info}Total reviews processed: {len(all_scores)}")
                print(f"{thread_info}Results by traveler type:")
                for traveler_type, scores in sorted(traveler_scores.items()):
                    avg = sum(scores) / len(scores) if scores else 0
                    print(f"{thread_info}  {traveler_type}: {avg:.2f} (from {len(scores)} reviews)")

                # Function to normalize traveler type names to valid field names
                def normalize_traveler_type(traveler_type):
                    normalized = traveler_type.lower().replace(' ', '_').replace('-', '_')
                    mappings = {
                        'couple': 'couples',
                        'group': 'groups_friends',
                        'solo_traveler': 'solo_travelers',
                        'solo_traveller': 'solo_travelers',
                        'group_of_friends': 'groups_friends',
                        'families': 'families',
                        'family': 'families'
                    }
                    return mappings.get(normalized, normalized)

                # Dynamically process each traveler type found
                for traveler_type, scores in traveler_scores.items():
                    if scores and traveler_type != "Unknown":
                        normalized_type = normalize_traveler_type(traveler_type)
                        score_field = f'avg_review_score_{normalized_type}'
                        count_field = f'avg_review_score_{normalized_type}_count'

                        # Add fields to property_data if they don't exist
                        if score_field not in property_data:
                            property_data[score_field] = 0
                        if count_field not in property_data:
                            property_data[count_field] = 0

                        property_data[score_field] = sum(scores) / len(scores)
                        property_data[count_field] = len(scores)

                        print(
                            f"{thread_info}Mapped '{traveler_type}' -> {score_field}: {property_data[score_field]:.2f} ({property_data[count_field]} reviews)")

                # Check if "BUSINESS_TRAVELLERS" option exists and process if available
                try:
                    select = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="customerType"]'))
                    )
                    available_options = [opt.get_attribute('value') for opt in
                                         select.find_elements(By.TAG_NAME, 'option')]

                    if "BUSINESS_TRAVELLERS" in available_options:
                        print(f"{thread_info}Processing 'BUSINESS_TRAVELLERS' category...")
                        business_scores, _ = process_reviews_for_category("BUSINESS_TRAVELLERS", "BUSINESS_TRAVELLERS")

                        if business_scores:
                            business_avg = sum(business_scores) / len(business_scores)
                            property_data['avg_review_score_business_travellers'] = business_avg
                            property_data['avg_review_score_business_travellers_count'] = len(business_scores)
                            print(
                                f"{thread_info}Business travelers: {business_avg:.2f} (from {len(business_scores)} reviews)")
                        else:
                            print(f"{thread_info}BUSINESS_TRAVELLERS option exists but no reviews found")
                    else:
                        print(f"{thread_info}BUSINESS_TRAVELLERS option not available for this property")

                except Exception as e:
                    print(f"{thread_info}Error checking for BUSINESS_TRAVELLERS option: {e}")

        except Exception as e:
            print(f"{thread_info}Error scraping reviews: {e}")

        # EXTRACT COORDINATES AND LOCATION
        try:
            print(f"{thread_info}Extracting coordinates...")

            def extract_coordinates(page_source):
                patterns = [
                    r'"latitude":([0-9\.\-]+),"longitude":([0-9\.\-]+)',
                    r'"lat":([0-9\.\-]+),"lng":([0-9\.\-]+)',
                ]

                for pattern in patterns:
                    match = re.search(pattern, page_source)
                    if match:
                        try:
                            lat = float(match.group(1))
                            lon = float(match.group(2))
                            return (lat, lon)
                        except (ValueError, IndexError):
                            continue
                return (None, None)

            latitude, longitude = extract_coordinates(driver.page_source)
            if latitude and longitude:
                property_data['latitude'] = latitude
                property_data['longitude'] = longitude
                print(f"{thread_info}Coordinates: {latitude}, {longitude}")

                location_details = get_location_details(latitude, longitude, driver)
                if location_details:
                    property_data.update({
                        'address': location_details['address'],
                        'zone': location_details['zone'],
                        'city': location_details['city']
                    })
                    print(f"{thread_info}Location: {location_details['city']}, {location_details['zone']}")

        except Exception as e:
            print(f"{thread_info}Error extracting coordinates: {e}")

    except Exception as e:
        print(f"{thread_info}Error scraping property {property_url}: {e}")

    return property_data


def worker_thread(property_urls_chunk, thread_id, csv_writer, progress_queue, batch_size=5):
    """Worker function for each thread"""
    logger.info(f"Thread {thread_id}: Starting with {len(property_urls_chunk)} properties")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()

    # Initialize batch tracking
    current_batch = []
    total_processed = 0

    try:
        for i, property_url in enumerate(property_urls_chunk, 1):
            print(f"\n--- Thread {thread_id}: Processing property {i}/{len(property_urls_chunk)} ---")

            try:
                property_data = scrape_single_property_data(driver, property_url, thread_id)
                current_batch.append(property_data)
                total_processed += 1

                # Save batch when it reaches batch_size or it's the last property
                if len(current_batch) >= batch_size or i == len(property_urls_chunk):
                    is_first_batch = (thread_id == 1 and total_processed <= batch_size)
                    csv_writer.write_batch(current_batch, is_first_batch)

                    print(f"📊 Thread {thread_id}: Batch completed: {len(current_batch)} properties saved")
                    progress_queue.put(len(current_batch))

                    # Reset for next batch
                    current_batch = []

                # Small delay between properties
                time.sleep(2)

            except Exception as e:
                print(f"⚠️ Thread {thread_id}: Error processing property {property_url}: {e}")
                print("⭐ Continuing with next property...")
                continue

    except KeyboardInterrupt:
        print(f"\n⚠️ Thread {thread_id}: Interrupted by user")
        if current_batch:
            csv_writer.write_batch(current_batch)
            progress_queue.put(len(current_batch))

    except Exception as e:
        print(f"⚠️ Thread {thread_id}: Unexpected error: {e}")
        if current_batch:
            csv_writer.write_batch(current_batch)
            progress_queue.put(len(current_batch))

    finally:
        # Save any remaining data
        if current_batch:
            csv_writer.write_batch(current_batch)
            progress_queue.put(len(current_batch))

        driver.quit()
        logger.info(f"Thread {thread_id}: Completed - processed {total_processed} properties")


def scrape_all_properties_threaded(destinations, num_threads=3, batch_size=5):
    """
    Main function that combines URL generation, property URL scraping,
    and detailed data extraction for each property with THREADING and batch saving.

    Args:
        destinations: List of destination cities
        num_threads: Number of threads to use (default: 3)
        batch_size: Number of properties to process before saving to CSV (default: 5)
    """
    print("=== STARTING IMPROVED THREADED BOOKING SCRAPER WITH BATCH PROCESSING ===")

    # Step 1: Generate search URLs
    print(f"\nStep 1: Generating search URLs for destinations: {destinations}")
    search_urls = url_builder(destinations)
    print(f"Generated {len(search_urls)} search URLs")

    # Step 2: Scrape property URLs from search results
    print(f"\nStep 2: Scraping property URLs from search results")
    all_property_urls = scrape_booking_results(search_urls)

    # Flatten the property URLs into a single list
    property_urls = []
    for search_url, urls in all_property_urls.items():
        property_urls.extend(urls)

    print(f"Found {len(property_urls)} total properties to scrape")

    if not property_urls:
        print("No property URLs found. Exiting.")
        return

    # Step 3: Divide property URLs among threads
    print(f"\nStep 3: Dividing properties among {num_threads} threads")

    chunk_size = len(property_urls) // num_threads
    url_chunks = []

    for i in range(num_threads):
        start_idx = i * chunk_size
        if i == num_threads - 1:  # Last thread gets remaining URLs
            end_idx = len(property_urls)
        else:
            end_idx = start_idx + chunk_size
        url_chunks.append(property_urls[start_idx:end_idx])

    print(f"URL chunks: {[len(chunk) for chunk in url_chunks]}")

    # Step 4: Initialize thread-safe CSV writer and progress tracking
    csv_filename = f'booking_properties_improved_threaded_{"-".join(destinations)}.csv'
    csv_writer = ThreadSafeCSVWriter(csv_filename)
    progress_queue = Queue()

    # Step 5: Start threads using ThreadPoolExecutor
    print(f"\nStep 5: Starting {num_threads} threads for detailed property scraping...")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all worker threads
        futures = []
        for i, url_chunk in enumerate(url_chunks):
            if url_chunk:  # Only start thread if chunk has URLs
                future = executor.submit(worker_thread, url_chunk, i + 1, csv_writer, progress_queue, batch_size)
                futures.append(future)

        # Monitor progress
        total_processed = 0
        total_properties = len(property_urls)

        print(f"📈 Progress monitoring started...")
        print(f"📊 Total properties to process: {total_properties}")

        # Process progress updates as they come
        completed_futures = 0
        while completed_futures < len(futures):
            # Check for progress updates
            try:
                while not progress_queue.empty():
                    batch_count = progress_queue.get_nowait()
                    total_processed += batch_count
                    percentage = (total_processed / total_properties * 100)
                    print(
                        f"📈 Progress Update: {total_processed}/{total_properties} properties processed ({percentage:.1f}%)")
            except:
                pass

            # Check for completed threads
            completed_count = sum(1 for future in futures if future.done())
            if completed_count > completed_futures:
                completed_futures = completed_count
                print(f"✅ {completed_futures}/{len(futures)} threads completed")

            time.sleep(2)  # Check every 2 seconds

        # Wait for all threads to complete and handle any exceptions
        print(f"\n⏳ Waiting for all threads to complete...")
        for i, future in enumerate(as_completed(futures)):
            try:
                future.result()  # This will raise any exceptions from the thread
                print(f"✅ Thread {i + 1} completed successfully")
            except Exception as e:
                print(f"❌ Thread {i + 1} failed with error: {e}")

        # Collect any remaining progress updates
        while not progress_queue.empty():
            batch_count = progress_queue.get()
            total_processed += batch_count

    print(f"\n🎉 IMPROVED THREADED SCRAPING COMPLETED")
    print(f"📊 Total properties processed: {total_processed}/{len(property_urls)}")
    print(f"💾 Data saved to: {csv_filename}")
    print(f"📈 Success rate: {(total_processed / len(property_urls) * 100):.1f}%")


# Additional function from single-threaded version for comparison
def save_batch_to_csv(properties_data, csv_filename, is_first_batch=False):
    """
    Save a batch of properties data to CSV.
    (This is the original function from single-threaded version for reference)
    """
    if not properties_data:
        return

    # Get all possible fieldnames from all properties in this batch
    all_fieldnames = set()
    for property_data in properties_data:
        all_fieldnames.update(property_data.keys())

    # If not first batch, we need to get existing fieldnames from the file
    existing_fieldnames = set()
    if not is_first_batch:
        try:
            with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                existing_fieldnames = set(reader.fieldnames or [])
        except FileNotFoundError:
            # File doesn't exist, treat as first batch
            is_first_batch = True

    # Combine all fieldnames
    fieldnames = sorted(list(all_fieldnames.union(existing_fieldnames)))

    # Write mode: 'w' for first batch, 'a' for subsequent batches
    mode = 'w' if is_first_batch else 'a'

    with open(csv_filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only for first batch
        if is_first_batch:
            writer.writeheader()

        # Write all properties in this batch
        for property_data in properties_data:
            # Fill missing fields with default values
            csv_row = {}
            for field in fieldnames:
                if field in property_data:
                    csv_row[field] = property_data[field]
                else:
                    # Default values based on field type
                    if field == 'property_url':
                        csv_row[field] = ''
                    elif field in ['category', 'address', 'zone', 'city', 'WiFi Speed']:
                        csv_row[field] = None
                    else:
                        csv_row[field] = 0
            writer.writerow(csv_row)

    print(f"✅ Saved batch of {len(properties_data)} properties to {csv_filename}")


def scrape_all_properties_to_csv_single_threaded(destinations, batch_size=10):
    """
    Single-threaded version for comparison and fallback.
    Main function that combines URL generation, property URL scraping,
    and detailed data extraction for each property with batch saving.

    Args:
        destinations: List of destination cities
        batch_size: Number of properties to process before saving to CSV (default: 10)
    """
    print("=== STARTING SINGLE-THREADED BOOKING SCRAPER WITH BATCH PROCESSING ===")

    # Step 1: Generate search URLs
    print(f"\nStep 1: Generating search URLs for destinations: {destinations}")
    search_urls = url_builder(destinations)
    print(f"Generated {len(search_urls)} search URLs")

    # Step 2: Scrape property URLs from search results
    print(f"\nStep 2: Scraping property URLs from search results")
    all_property_urls = scrape_booking_results(search_urls)

    # Flatten the property URLs into a single list
    property_urls = []
    for search_url, urls in all_property_urls.items():
        property_urls.extend(urls)

    print(f"Found {len(property_urls)} total properties to scrape")

    if not property_urls:
        print("No property URLs found. Exiting.")
        return

    # Step 3: Scrape detailed data for each property with batch processing
    print(f"\nStep 3: Scraping detailed data for each property (batch size: {batch_size})")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    csv_filename = f'booking_properties_single_threaded_{"-".join(destinations)}.csv'

    # Initialize batch tracking
    current_batch = []
    batch_number = 1
    total_processed = 0

    try:
        for i, property_url in enumerate(property_urls, 1):
            print(f"\n--- Processing property {i}/{len(property_urls)} (Batch {batch_number}) ---")

            try:
                property_data = scrape_single_property_data(driver, property_url)
                current_batch.append(property_data)
                total_processed += 1

                # Save batch when it reaches batch_size or it's the last property
                if len(current_batch) >= batch_size or i == len(property_urls):
                    is_first_batch = batch_number == 1
                    save_batch_to_csv(current_batch, csv_filename, is_first_batch)

                    print(f"Batch {batch_number} completed: {len(current_batch)} properties saved")
                    print(
                        f"Progress: {total_processed}/{len(property_urls)} properties processed ({(total_processed / len(property_urls) * 100):.1f}%)")

                    # Reset for next batch
                    current_batch = []
                    batch_number += 1

                # Small delay between properties
                time.sleep(2)

            except Exception as e:
                print(f"Error processing property {property_url}: {e}")
                print("⭐Continuing with next property...")
                continue

    except KeyboardInterrupt:
        print(f"\n⚠️ Scraping interrupted by user")
        print(f"Saving current batch ({len(current_batch)} properties) before exit...")
        if current_batch:
            is_first_batch = batch_number == 1
            save_batch_to_csv(current_batch, csv_filename, is_first_batch)
            total_processed += len(current_batch)

    except Exception as e:
        print(f"Unexpected error during scraping: {e}")
        print(f"Saving current batch ({len(current_batch)} properties) before exit...")
        if current_batch:
            is_first_batch = batch_number == 1
            save_batch_to_csv(current_batch, csv_filename, is_first_batch)
            total_processed += len(current_batch)

    finally:
        driver.quit()
        print(f"\nSCRAPING COMPLETED")
        print(f"Total properties processed: {total_processed}/{len(property_urls)}")
        print(f"Data saved to: {csv_filename}")
        print(f"Success rate: {(total_processed / len(property_urls) * 100):.1f}%")


# Main execution
if __name__ == "__main__":
    # Specify the cities you want to scrape
    cities_to_scrape = ["Tangier"]  # You can add more cities: ["Tangier", "Marrakech", "Casablanca"]

    print("Choose scraping mode:")
    print("1. Multi-threaded (faster, recommended)")
    print("2. Single-threaded (more stable, fallback)")

    choice = input("Enter your choice (1 or 2): ").strip()

    if choice == "2":
        print("\n🔄 Running SINGLE-THREADED version...")
        # Run the single-threaded scraper
        scrape_all_properties_to_csv_single_threaded(cities_to_scrape, batch_size=5)
    else:
        print("\n🚀 Running MULTI-THREADED version...")
        # Run the improved multi-threaded scraper
        # num_threads=3 is recommended for i7 11th gen with 16GB RAM
        # batch_size=5 means each thread saves every 5 properties
        scrape_all_properties_threaded(cities_to_scrape, num_threads=3, batch_size=5)