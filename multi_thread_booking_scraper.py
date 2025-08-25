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
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from collections import defaultdict
import requests
from datetime import date, timedelta, datetime
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Global lock for CSV writing
csv_lock = threading.Lock()

# === TESTING LIMITS ===
# Set these to None or 0 to disable the limits
TEST_MAX_PROPERTIES = 15        # scrape only first 15 properties
TEST_MAX_REVIEW_PAGES = 2       # first page + one extra page (click next once)



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


def scrape_property_urls(urls):
    """Scrape property URLs from search results"""
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    all_urls = []
    seen = set()  # Track canonical property URLs to avoid duplicates

    try:
        for search_url in urls:
            print(f"Navigating to: {search_url}")
            driver.get(search_url)

            # Handle cookie consent
            try:
                accept_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                accept_btn.click()
            except TimeoutException:
                pass

            # Load all results
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                try:
                    more_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, ".//span[contains(text(), ' more results')]"))
                    )
                    more_btn.click()
                    time.sleep(3)
                except (NoSuchElementException, TimeoutException):
                    break

            # Extract property URLs
            links = driver.find_elements(By.XPATH, '//a[@data-testid="title-link"]')
            for link in links:
                href = link.get_attribute('href')
                if href:
                    # Keep query parameters (checkin, checkout, guests, etc.) in the URL so that
                    # subsequent price scraping loads the correct availability context. We still
                    # deduplicate by the canonical part of the URL.
                    canonical = href.split('?')[0]
                    if canonical not in seen:
                        seen.add(canonical)
                        all_urls.append(href)

            print(f"Found {len([l for l in links if l.get_attribute('href')])} properties")

    finally:
        driver.quit()

    # Limit number of properties during testing
    if TEST_MAX_PROPERTIES:
        return all_urls[:TEST_MAX_PROPERTIES]
    return all_urls


def normalize_traveler_type(traveler_type):
    """Normalize traveler type names to valid field names"""
    normalized = traveler_type.lower().replace(' ', '_').replace('-', '_')
    mappings = {
        'couple': 'couples',
        'group': 'groups_friends',
        'solo_traveler': 'solo_travelers',
        'solo_traveller': 'solo_travelers',
        'group_of_friends': 'groups_friends',
        'families': 'families',
        'family': 'families',
        'business_traveller': 'business_travellers',
        'business_traveler': 'business_travellers'
    }
    return mappings.get(normalized, normalized)


def process_reviews_by_traveler_type(driver, prefix=""):
    """Process all reviews and categorize by traveler type"""
    traveler_scores = defaultdict(list)

    try:
        # Select "ALL" customer type to get all reviews with traveler types
        select = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="customerType"]'))
        )
        select_element = Select(select)
        select_element.select_by_value("ALL")
        print(f"{prefix}Selected 'ALL' customer type")
        time.sleep(2)

        page_count = 0
        while True:
            page_count += 1
            print(f"{prefix}Processing reviews page {page_count}")

            try:
                # Wait for review cards to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="review-card"]'))
                )
                review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-testid="review-card"]')
                print(f"{prefix}Found {len(review_cards)} reviews on page {page_count}")

                # Process each review card
                for i, card in enumerate(review_cards):
                    try:
                        # Extract score
                        score_text = card.find_element(By.XPATH, './/div[contains(text(), "Scored")]').text
                        score = float(score_text.split("Scored ")[1].strip())

                        # Extract traveler type
                        traveler_type = "Unknown"
                        try:
                            traveler_element = card.find_element(By.CSS_SELECTOR,
                                                                 '[data-testid="review-traveler-type"]')
                            traveler_type = traveler_element.text.strip() or "Unknown"
                        except:
                            pass

                        # Store score by traveler type
                        if traveler_type != "Unknown":
                            traveler_scores[traveler_type].append(score)

                    except Exception as e:
                        print(f"{prefix}Error processing review card {i + 1}: {e}")

                # Stop after limited pages in testing mode
                if TEST_MAX_REVIEW_PAGES and page_count >= TEST_MAX_REVIEW_PAGES:
                    print(f"{prefix}Reached testing limit of review pages ({TEST_MAX_REVIEW_PAGES})")
                    break

                # Try to go to next page
                try:
                    next_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//*[@id="reviewCardsSection"]/div[2]/div[1]/div/div/div[3]/button'))
                    )

                    if "disabled" in next_btn.get_attribute("class"):
                        print(f"{prefix}Reached last page")
                        break

                    next_btn.click()
                    time.sleep(2)
                    print(f"{prefix}Moved to next page")

                except:
                    print(f"{prefix}No next page available")
                    break

            except Exception as e:
                print(f"{prefix}Error processing page {page_count}: {e}")
                break

        # Process specific traveler categories if available
        try:
            select = driver.find_element(By.CSS_SELECTOR, 'select[name="customerType"]')
            available_options = [opt.get_attribute('value') for opt in select.find_elements(By.TAG_NAME, 'option')]

            if "BUSINESS_TRAVELLERS" in available_options:
                business_scores = process_specific_traveler_category(driver, "BUSINESS_TRAVELLERS", prefix)
                if business_scores:
                    traveler_scores["Business travellers"].extend(business_scores)
        except Exception as e:
            print(f"{prefix}Error processing specific categories: {e}")

    except Exception as e:
        print(f"{prefix}Error in traveler type processing: {e}")

    return dict(traveler_scores)


def process_specific_traveler_category(driver, category_value, prefix=""):
    """Process reviews for a specific traveler category"""
    scores = []

    try:
        select = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="customerType"]'))
        )
        select_element = Select(select)
        select_element.select_by_value(category_value)
        print(f"{prefix}Processing {category_value} reviews")
        time.sleep(2)

        page_count = 0
        while True:
            page_count += 1
            # Stop after limited pages when testing
            if TEST_MAX_REVIEW_PAGES and page_count > TEST_MAX_REVIEW_PAGES:
                print(f"{prefix}Reached review page limit ({TEST_MAX_REVIEW_PAGES})")
                break
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="review-card"]'))
                )
                review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-testid="review-card"]')

                for card in review_cards:
                    try:
                        score_text = card.find_element(By.XPATH, './/div[contains(text(), "Scored")]').text
                        score = float(score_text.split("Scored ")[1].strip())
                        scores.append(score)
                    except:
                        pass

                # Try next page
                try:
                    next_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//*[@id="reviewCardsSection"]/div[2]/div[1]/div/div/div[3]/button'))
                    )
                    if "disabled" in next_btn.get_attribute("class"):
                        break
                    next_btn.click()
                    time.sleep(2)
                except:
                    break
            except:
                break

    except Exception as e:
        print(f"{prefix}Error processing {category_value}: {e}")

    return scores
    """Get location details from coordinates"""
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&accept-language=en"
        response = requests.get(url, headers={'User-Agent': 'BookingScraper/1.0'}, timeout=10)
        response.raise_for_status()
        data = response.json()

        address_components = data.get('address', {})
        latin_pattern = re.compile(r'[^a-zA-Z0-9\s\-,\.\']')

        # Clean address
        address = latin_pattern.sub('', data.get('display_name', '')).strip()
        if address:
            address = address.replace(',', ' ')

        # Get zone
        zone = None
        zone_fields = ['neighbourhood', 'suburb', 'quarter', 'city_district', 'district']
        for field in zone_fields:
            if field in address_components and address_components[field]:
                zone = latin_pattern.sub('', address_components[field]).strip()
                if zone:
                    break

        # Get city
        city = None
        city_fields = ['city', 'town', 'municipality', 'village']
        for field in city_fields:
            if field in address_components and address_components[field]:
                city = address_components[field].strip()
                break

        return {'address': address, 'zone': zone, 'city': city}

    except Exception as e:
        print(f"Error getting location: {e}")
        return {'address': None, 'zone': None, 'city': None}


def extract_prices(driver):
    """Return (min_price, max_price) from current Booking.com property page.
    Strategy:
    1. Wait for price table cells to appear and scrape helper spans (current Booking markup).
    2. Fallback to a generic selector list.
    3. Final fallback: regex on page source.
    """

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

        # Extract zone (neighbourhood/suburb…)
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


def scrape_property_data(driver, url, thread_id=None):
    """Scrape detailed data for a single property"""
    prefix = f"Thread {thread_id}: " if thread_id else ""
    print(f"{prefix}Scraping: {url}")

    data = {
        'property_id': str(uuid.uuid4()),
        'scrape_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'property_url': url,
        'category': None,
        'general_review': None,
        'general_review_count': None,
        'comfort_score': None,
        'value_score': None,
        'location_score': None,
        'wifi_score': None,
        'avg_review_score_all': None,
        'avg_review_score_all_count': None,
        'avg_review_score_families': None,
        'avg_review_score_families_count': None,
        'avg_review_score_couples': None,
        'avg_review_score_couples_count': None,
        'avg_review_score_solo_travelers': None,
        'avg_review_score_solo_travelers_count': None,
        'avg_review_score_business_travellers': None,
        'avg_review_score_business_travellers_count': None,
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

        # Extract reviews and process by traveler type
        try:
            # Click the reviews link/score card and handle possible new window/tab
            parent_handle = driver.current_window_handle
            handles_before = driver.window_handles

            # Try multiple selectors because Booking may render the button differently per property
            review_selectors = [
                (By.XPATH, "//*[@id='js--hp-gallery-scorecard']"),  # score card at the top of gallery (xpath)
                (By.CSS_SELECTOR, "a[data-testid='see-all-reviews-link']"),  # explicit "See all reviews" link
                (By.CSS_SELECTOR, "a[href*='#tab-reviews']"),  # anchor link within the same page
            ]

            clicked = False
            for by, selector in review_selectors:
                try:
                    review_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "js--hp-gallery-scorecard"))
                    )
                    review_btn.click()
                    clicked = True
                    break
                except Exception:
                    continue  # try next selector

            if not clicked:
                print(f"{prefix}Unable to locate reviews link with known selectors")
                raise Exception("Reviews link not found")

            # Wait a moment for potential new window/tab to appear and identify it
            time.sleep(2)
            handles_after = driver.window_handles
            new_window = None
            for h in handles_after:
                if h not in handles_before:
                    new_window = h
                    break

            if new_window:
                driver.switch_to.window(new_window)

            # Ensure the reviews section has loaded in the active window (new or same)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="review-card"]'))
            )

            # Extract basic scores
            score_xpaths = [
                ('comfort_score', '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[4]'),
                ('value_score', '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[5]'),
                ('location_score', '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[6]'),
                ('wifi_score', '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[7]'),
            ]

            for score_key, xpath in score_xpaths:
                try:
                    element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, xpath)))
                    data[score_key] = float(element.get_attribute("textContent"))
                except:
                    pass

            # General score and count
            try:
                general_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]'))
                )
                data['general_review'] = float(general_element.get_attribute("textContent"))
            except:
                pass

            try:
                count_element = driver.find_element(By.XPATH,
                                                    '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[4]/div[2]')
                count_text = ''.join(filter(str.isdigit, count_element.text))
                if count_text:
                    data['general_review_count'] = int(count_text)
            except:
                pass

            # Process reviews by traveler type
            print(f"{prefix}Processing reviews by traveler type...")
            traveler_scores = process_reviews_by_traveler_type(driver, prefix)

            # Update data with traveler type averages
            for traveler_type, scores in traveler_scores.items():
                if scores:
                    normalized_type = normalize_traveler_type(traveler_type)
                    score_field = f'avg_review_score_{normalized_type}'
                    count_field = f'avg_review_score_{normalized_type}_count'

                    # Ensure the field exists in our data structure
                    data[score_field] = sum(scores) / len(scores)
                    data[count_field] = len(scores)

                    print(f"{prefix}{traveler_type} -> {score_field}: {data[score_field]:.2f} ({len(scores)} reviews)")

            # Also set the 'all' category data if we have traveler scores
            all_scores = []
            for scores in traveler_scores.values():
                all_scores.extend(scores)

            if all_scores:
                data['avg_review_score_all'] = sum(all_scores) / len(all_scores)
                data['avg_review_score_all_count'] = len(all_scores)
                print(f"{prefix}All travelers: {data['avg_review_score_all']:.2f} ({len(all_scores)} reviews)")

            # Close the reviews tab/window and switch back to property page if we opened a new one
            if new_window:
                try:
                    driver.close()
                except Exception:
                    pass
                driver.switch_to.window(parent_handle)

        except Exception as e:
            print(f"{prefix}Error extracting reviews: {e}")

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
        'comfort_score',
        'value_score',
        'location_score',
        'wifi_score',
        'avg_review_score_all',
        'avg_review_score_all_count',
        'avg_review_score_families',
        'avg_review_score_families_count',
        'avg_review_score_couples',
        'avg_review_score_couples_count',
        'avg_review_score_solo_travelers',
        'avg_review_score_solo_travelers_count',
        'avg_review_score_business_travellers',
        'avg_review_score_business_travellers_count',
        'avg_review_score_groups_friends',
        'avg_review_score_groups_friends_count',
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

    # Use predefined field order
    base_fields = get_all_possible_fields()

    # Also collect any dynamic fields from the data
    dynamic_fields = set()
    for item in data_list:
        for key in item.keys():
            if key not in base_fields:
                dynamic_fields.add(key)

    # Combine base fields with any new dynamic fields
    fieldnames = base_fields + sorted(list(dynamic_fields))

    with csv_lock:
        # Check if file exists and get existing headers
        file_exists = False
        existing_fieldnames = []

        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                existing_fieldnames = reader.fieldnames or []
                file_exists = True
        except FileNotFoundError:
            pass

        # If file exists, merge fieldnames to include any new fields
        if file_exists and existing_fieldnames:
            all_fieldnames = list(existing_fieldnames)
            for field in fieldnames:
                if field not in all_fieldnames:
                    all_fieldnames.append(field)
            fieldnames = all_fieldnames

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
                        if field == 'property_url':
                            row[field] = item.get('property_url', '')
                        elif field in ['category', 'address', 'zone', 'city', 'wifi_speed']:
                            row[field] = ''  # Empty string instead of None for text fields
                        elif field in ['latitude', 'longitude']:
                            row[field] = ''  # Empty string for coordinates
                        else:
                            row[field] = 0  # Zero for numeric fields

                writer.writerow(row)

    print(f"Saved {len(data_list)} properties to {filename}")


def worker_thread(urls_chunk, thread_id, filename, batch_size=5):
    """Worker function for threading"""
    print(f"Thread {thread_id}: Starting with {len(urls_chunk)} properties")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()

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
    property_urls = scrape_property_urls(search_urls)
    print(f"Found {len(property_urls)} properties")

    if not property_urls:
        print("No properties found")
        return

    # Divide URLs among threads
    chunk_size = len(property_urls) // num_threads
    url_chunks = []

    for i in range(num_threads):
        start = i * chunk_size
        end = len(property_urls) if i == num_threads - 1 else start + chunk_size
        if start < len(property_urls):
            url_chunks.append(property_urls[start:end])

    print(f"Divided into {len(url_chunks)} chunks: {[len(chunk) for chunk in url_chunks]}")

    # Setup output file
    filename = f'booking_properties_{"-".join(destinations).lower()}.csv'

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
    property_urls = scrape_property_urls(search_urls)

    if not property_urls:
        print("No properties found")
        return

    print(f"Found {len(property_urls)} properties")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    filename = f'booking_properties_single_{"-".join(destinations).lower()}.csv'

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
    cities = ["Marrakesh"]

    print("Choose scraping mode:")
    print("1. Multi-threaded (faster)")
    print("2. Single-threaded (more stable)")

    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "2":
        scrape_single_threaded(cities, batch_size=5)
    else:
        scrape_booking_properties(cities, num_threads=3, batch_size=5)