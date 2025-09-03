import re
import time
import csv
import threading
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
import requests
from datetime import date, timedelta, datetime
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Global lock for CSV writing
csv_lock = threading.Lock()

# === TESTING LIMITS ===
# Set these to None or 0 to disable the limits
TEST_MAX_PROPERTIES = 200  # scrape only first 200 properties


def init_driver():
    """Initialize and return a remote Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless=new')  # Ensure headless mode is enabled for server environments
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")

    # Connect to the Selenium Hub/Node using the service name from docker-compose.yml
    selenium_url = os.environ.get('SELENIUM_URL', 'http://selenium:4444/wd/hub')

    driver = webdriver.Remote(
        command_executor=selenium_url,
        options=chrome_options
    )

    # Set timeouts
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)

    # Execute script to remove webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver


def build_urls(destinations):
    """Build search URLs for multiple destinations"""
    base_url = "https://www.booking.com/searchresults.html?"
    today = date.today()
    tomorrow = today + timedelta(days=1)

    urls = []
    for city in destinations:
        params = {
            'ss': city,
            'checkin': today.strftime("%Y-%m-%d"),
            'checkout': tomorrow.strftime("%Y-%m-%d"),
            'group_adults': 1,
            'no_rooms': 1,
            'group_children': 0,
        }
        url_parts = [f"{key}={value}" for key, value in params.items()]
        urls.append(base_url + "&".join(url_parts))

    return urls


def scrape_property_urls(urls, max_links=500):
    """Scrape property URLs from search results until reaching max_links"""
    driver = init_driver()
    all_urls = []
    seen = set()  # Track canonical property URLs to avoid duplicates

    try:
        for search_url in urls:
            if len(all_urls) >= max_links:
                break

            print(f"Navigating to: {search_url}")
            driver.get(search_url)
            time.sleep(3)  # Give page more time to load

            # Handle cookie consent
            try:
                accept_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                accept_btn.click()
                print("Cookie consent accepted")
            except TimeoutException:
                print("No cookie consent button found")

            # Try multiple selectors for property links
            property_selectors = [
                '//a[@data-testid="title-link"]',
                '//h3[@data-testid="title"]/a',
                '//div[@data-testid="property-card"]//a[contains(@href, "/hotel/")]',
                '//a[contains(@class, "e13098a59f") and contains(@href, "/hotel/")]',
                '//a[contains(@href, "/hotel/") and not(contains(@href, "#"))]',
                '//div[contains(@class, "sr_property_block")]//a[contains(@class, "hotel_name_link")]',
                '//a[contains(@class, "js-sr-hotel-link")]',
            ]

            # Initial wait and scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

            scroll_attempts = 0
            max_scroll_attempts = 10

            while scroll_attempts < max_scroll_attempts:
                if len(all_urls) >= max_links:
                    break

                scroll_attempts += 1
                print(f"Scroll attempt {scroll_attempts}/{max_scroll_attempts}")

                # Try to find property links with various selectors
                links_found = False
                for selector in property_selectors:
                    try:
                        links = driver.find_elements(By.XPATH, selector)
                        if links:
                            print(f"Found {len(links)} links with selector: {selector}")
                            links_found = True

                            for link in links:
                                if len(all_urls) >= max_links:
                                    break

                                try:
                                    href = link.get_attribute('href')
                                    if href and '/hotel/' in href:
                                        canonical = href.split('?')[0]
                                        if canonical not in seen:
                                            seen.add(canonical)
                                            all_urls.append(href)
                                except Exception as e:
                                    print(f"Error extracting href: {e}")
                            break
                    except Exception as e:
                        continue

                if not links_found:
                    print("No property links found with any selector")

                    # Check if we're on a captcha or error page
                    page_text = driver.find_element(By.TAG_NAME, 'body').text.lower()
                    if 'captcha' in page_text or 'verify' in page_text:
                        print("WARNING: Possible captcha detected")
                    elif 'no properties found' in page_text or 'no results' in page_text:
                        print("No properties found for this search")
                        break

                # Scroll down
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                # Try to click "Load more" button
                try:
                    load_more_selectors = [
                        "//button[contains(text(), 'Load more')]",
                        "//button[contains(text(), 'Show more')]",
                        "//span[contains(text(), 'more results')]",
                        "//button[@data-testid='pagination-next-btn']"
                    ]

                    for btn_selector in load_more_selectors:
                        try:
                            more_btn = driver.find_element(By.XPATH, btn_selector)
                            if more_btn.is_displayed() and more_btn.is_enabled():
                                driver.execute_script("arguments[0].click();", more_btn)
                                print(f"Clicked load more button: {btn_selector}")
                                time.sleep(3)
                                break
                        except:
                            continue
                except:
                    pass

                print(f"Collected {len(all_urls)} unique properties so far")

                # If we haven't found any links after several attempts, break
                if scroll_attempts > 3 and len(all_urls) == 0:
                    print("No properties found after multiple attempts, moving to next destination")
                    break

    except Exception as e:
        print(f"Error in scrape_property_urls: {e}")
    finally:
        driver.quit()

    return all_urls


def get_location_details(lat, lon):
    """Reverse-geocode latitude/longitude to address, zone and city (using Nominatim)."""
    try:
        url = (
            "https://nominatim.openstreetmap.org/reverse?format=json"
            f"&lat={lat}&lon={lon}&accept-language=en"
        )
        headers = {
            "User-Agent": "BookingScraper/1.0 (contact@example.com)"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        address_components = data.get("address", {})
        latin_pattern = re.compile(r"[^a-zA-Z0-9\s\-,\.']")

        # Raw display name cleaned
        address = latin_pattern.sub("", data.get("display_name", "")).strip()
        if address:
            address = address.replace(",", " ")

        # Extract zone (neighbourhood/suburb...)
        zone = None
        for field in [
            "neighbourhood",
            "suburb",
            "quarter",
            "city_district",
            "district",
        ]:
            if field in address_components and address_components[field]:
                zone = latin_pattern.sub("", address_components[field]).strip()
                if zone:
                    break

        # Extract city
        city = None
        for field in ["city", "town", "municipality", "village"]:
            if field in address_components and address_components[field]:
                city = address_components[field].strip()
                break

        return {"address": address, "zone": zone, "city": city}

    except Exception as e:
        print(f"Error getting location: {e}")
        return {"address": None, "zone": None, "city": None}


def extract_prices(driver):
    """Return (min_price, max_price) from current Booking.com property page."""
    prices = []

    # --- Primary (current markup) ---
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "td.hprt-table-cell-price"))
        )
        price_elements = driver.find_elements(
            By.CSS_SELECTOR,
            "td.hprt-table-cell-price div.hprt-price-block div.prco-wrapper span.prco-valign-middle-helper",
        )
        for el in price_elements:
            txt = el.text.strip()
            if not txt:
                continue
            num = "".join(filter(str.isdigit, txt))
            if num:
                try:
                    prices.append(int(num))
                except ValueError:
                    pass
    except Exception:
        pass  # timeout or structure changed – continue with fallbacks

    # --- Fallback: generic selectors ---
    if not prices:
        generic_selectors = [
            "td.hp-price-left-align.hprt-table-cell.hprt-table-cell-price div.hprt-price-block span.prc-no-css",
            "td.hprt-table-cell-price span.prc-no-css",
            "div.hprt-price-block span.prc-no-css",
            "span[data-testid='price-and-discounted-price']",
            "div[data-testid='price-and-discounted-price']",
            "span.hprt-price-price-standard",
            "span.fcab3ed991.bd73d13072",
        ]
        for css in generic_selectors:
            for el in driver.find_elements(By.CSS_SELECTOR, css):
                txt = el.text.strip()
                if not txt:
                    continue
                digits = re.findall(r"\d+", txt.replace(",", ""))
                if digits:
                    try:
                        prices.append(int("".join(digits)))
                    except ValueError:
                        pass

    # --- Final fallback: regex over HTML ---
    if not prices:
        matches = re.findall(r"[€$£]\s?(\d{2,5})", driver.page_source)
        prices.extend([int(m) for m in matches])

    if not prices:
        return None, None
    return min(prices), max(prices)


def extract_category(driver):
    """Extract property category"""
    try:
        element = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="breadcrumb-current"] span'))
        )
        text = element.text.strip()

        # Extract category from the SECOND pair of parentheses counting from the end.
        matches = re.findall(r'\(([^)]+)\)', text)
        if len(matches) >= 2:
            category = matches[-2]  # second from the end
        elif matches:
            category = matches[-1]  # only one pair present
        else:
            category = text

        # Normalize categories
        if category == 'Guest House':
            return 'Riad'
        elif category == 'Condo Hotel':
            return 'Apartment-Hotel'
        return category
    except:
        return None


def extract_coordinates(page_source):
    """Extract coordinates from page source"""
    patterns = [
        r'"latitude":([0-9\.\-]+),"longitude":([0-9\.\-]+)',
        r'"lat":([0-9\.\-]+),"lng":([0-9\.\-]+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_source)
        if match:
            try:
                return float(match.group(1)), float(match.group(2))
            except (ValueError, IndexError):
                continue
    return None, None


def scrape_property_data(driver, url, thread_id=None):
    """Scrape basic data for a single property"""
    prefix = f"Thread {thread_id}: " if thread_id else ""
    print(f"{prefix}Scraping: {url}")

    data = {
        'property_id': str(uuid.uuid4()),
        'scrape_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'property_url': url,
        'category': None,
        'general_review': None,
        'general_review_count': None,
        'wifi_score': None,
        'min_price': None,
        'max_price': None,
        'latitude': None,
        'longitude': None,
        'address': None,
        'zone': None,
        'city': None,
        'wifi_speed': None,
    }

    try:
        driver.get(url)
        time.sleep(2)

        # Extract category
        data['category'] = extract_category(driver)

        # Extract prices (min_price & max_price)
        try:
            min_p, max_p = extract_prices(driver)
            data['min_price'] = min_p
            data['max_price'] = max_p
        except Exception as e:
            print(f"{prefix}Error extracting prices: {e}")

        # Extract WiFi speed
        try:
            speed_element = driver.find_element(By.XPATH, "//div[contains(text(), 'Mbps')]")
            data['wifi_speed'] = speed_element.text.split('•')[-1].strip()
        except:
            data['wifi_speed'] = 'Not specified'

        # Extract basic review info
        try:
            # General review score
            general_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]'))
            )
            data['general_review'] = float(general_element.get_attribute("textContent"))
        except:
            pass

        try:
            # General review count
            count_element = driver.find_element(By.XPATH,
                                                '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[4]/div[2]')
            count_text = ''.join(filter(str.isdigit, count_element.text))
            if count_text:
                data['general_review_count'] = int(count_text)
        except:
            pass

        # Extract WiFi score from reviews section
        try:
            # Navigate to reviews to get WiFi score
            review_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='js--hp-gallery-scorecard']"))
            )
            review_btn.click()
            time.sleep(2)

            # Extract WiFi score
            wifi_element = driver.find_element(
                By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[7]'
            )
            data['wifi_score'] = float(wifi_element.get_attribute("textContent"))
        except Exception as e:
            print(f"{prefix}Error extracting WiFi score: {e}")

        # Extract coordinates and location
        try:
            lat, lon = extract_coordinates(driver.page_source)
            if lat and lon:
                data['latitude'] = lat
                data['longitude'] = lon

                location = get_location_details(lat, lon)
                data.update(location)
        except Exception as e:
            print(f"{prefix}Error extracting location: {e}")

    except Exception as e:
        print(f"{prefix}Error scraping property: {e}")

    return data


def get_all_possible_fields():
    """Define all possible CSV fields to ensure consistent column ordering"""
    return [
        'property_id',
        'scrape_timestamp',
        'property_url',
        'category',
        'general_review',
        'general_review_count',
        'wifi_score',
        'min_price',
        'max_price',
        'latitude',
        'longitude',
        'address',
        'zone',
        'city',
        'wifi_speed'
    ]


def save_to_csv(data_list, filename):
    """Thread-safe CSV writing with proper field handling"""
    if not data_list:
        return

    fieldnames = get_all_possible_fields()

    with csv_lock:
        # Check if file exists
        file_exists = False
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                file_exists = True
        except FileNotFoundError:
            pass

        mode = 'a' if file_exists else 'w'

        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header only for new files
            if not file_exists:
                writer.writeheader()

            for item in data_list:
                # Prepare row with proper default values
                row = {}
                for field in fieldnames:
                    if field in item and item[field] is not None:
                        row[field] = item[field]
                    else:
                        # Set appropriate default values
                        if field in ['category', 'address', 'zone', 'city', 'wifi_speed']:
                            row[field] = ''  # Empty string for text fields
                        elif field in ['latitude', 'longitude']:
                            row[field] = ''  # Empty string for coordinates
                        else:
                            row[field] = 0  # Zero for numeric fields

                writer.writerow(row)

    print(f"Saved {len(data_list)} properties to {filename}")


def worker_thread(urls_chunk, thread_id, filename, batch_size=5):
    """Worker function for threading"""
    print(f"Thread {thread_id}: Starting with {len(urls_chunk)} properties")

    driver = init_driver()

    batch = []
    processed = 0

    try:
        for i, url in enumerate(urls_chunk, 1):
            try:
                data = scrape_property_data(driver, url, thread_id)
                batch.append(data)
                processed += 1

                # Save batch when full or last item
                if len(batch) >= batch_size or i == len(urls_chunk):
                    save_to_csv(batch, filename)
                    batch = []

                time.sleep(1)  # Small delay between requests

            except Exception as e:
                print(f"Thread {thread_id}: Error processing {url}: {e}")
                continue

    except KeyboardInterrupt:
        print(f"Thread {thread_id}: Interrupted")
        if batch:
            save_to_csv(batch, filename)

    finally:
        if batch:
            save_to_csv(batch, filename)
        driver.quit()
        print(f"Thread {thread_id}: Completed - processed {processed} properties")


def scrape_booking_properties(destinations, num_threads=3, batch_size=5):
    """Main scraping function"""
    print("=== BOOKING.COM SCRAPER ===")

    # Generate URLs
    print(f"Generating URLs for: {destinations}")
    search_urls = build_urls(destinations)

    # Get property URLs
    print("Scraping property URLs...")

    # Apply testing limit if set
    max_properties = TEST_MAX_PROPERTIES if TEST_MAX_PROPERTIES else 500
    property_urls = scrape_property_urls(search_urls, max_links=max_properties)

    print(f"Found {len(property_urls)} properties")

    if not property_urls:
        print("No properties found")
        print("\nPossible reasons:")
        print("1. Booking.com has changed their HTML structure")
        print("2. Anti-bot detection is blocking the scraper")
        print("3. The search returned no results")
        print("\nTry:")
        print("- Running with a VPN or proxy")
        print("- Adding more delays between requests")
        print("- Checking if the cities have properties on Booking.com")
        return

    # Divide URLs among threads
    chunk_size = len(property_urls) // num_threads if num_threads > 0 else len(property_urls)
    url_chunks = []

    for i in range(num_threads):
        start = i * chunk_size
        end = len(property_urls) if i == num_threads - 1 else start + chunk_size
        if start < len(property_urls):
            url_chunks.append(property_urls[start:end])

    print(f"Divided into {len(url_chunks)} chunks: {[len(chunk) for chunk in url_chunks]}")

    # Setup output file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'/app/results/booking_properties_{"-".join(destinations).lower()}_{timestamp}.csv'

    # Start threads
    print(f"Starting {len(url_chunks)} threads...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, chunk in enumerate(url_chunks):
            future = executor.submit(worker_thread, chunk, i + 1, filename, batch_size)
            futures.append(future)

        # Wait for completion
        for i, future in enumerate(as_completed(futures)):
            try:
                future.result()
                print(f"Thread {i + 1} completed successfully")
            except Exception as e:
                print(f"Thread {i + 1} failed: {e}")

    print(f"\n=== SCRAPING COMPLETED ===")
    print(f"Results saved to: {filename}")


def scrape_single_threaded(destinations, batch_size=10):
    """Single-threaded version for comparison"""
    print("=== SINGLE-THREADED SCRAPER ===")

    search_urls = build_urls(destinations)

    # Apply testing limit if set
    max_properties = TEST_MAX_PROPERTIES if TEST_MAX_PROPERTIES else 500
    property_urls = scrape_property_urls(search_urls, max_links=max_properties)

    if not property_urls:
        print("No properties found")
        return

    print(f"Found {len(property_urls)} properties")

    driver = init_driver()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'/app/results/booking_properties_single_{"-".join(destinations).lower()}_{timestamp}.csv'

    batch = []
    processed = 0

    try:
        for i, url in enumerate(property_urls, 1):
            print(f"Processing {i}/{len(property_urls)}")

            try:
                data = scrape_property_data(driver, url)
                batch.append(data)
                processed += 1

                if len(batch) >= batch_size or i == len(property_urls):
                    save_to_csv(batch, filename)
                    print(
                        f"Saved batch. Progress: {processed}/{len(property_urls)} ({processed / len(property_urls) * 100:.1f}%)")
                    batch = []

                time.sleep(1)

            except Exception as e:
                print(f"Error processing {url}: {e}")
                continue

    except KeyboardInterrupt:
        print("Interrupted by user")
        if batch:
            save_to_csv(batch, filename)

    finally:
        if batch:
            save_to_csv(batch, filename)
        driver.quit()
        print(f"\nCompleted: {processed}/{len(property_urls)} properties")
        print(f"Results saved to: {filename}")


if __name__ == "__main__":
    cities = ["Marrakech", "Tangier"]
    scrape_single_threaded(cities, batch_size=5)