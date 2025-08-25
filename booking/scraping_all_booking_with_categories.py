import logging
import random
import time
import re
import csv
import json
import requests
from datetime import datetime, timedelta
from urllib.parse import quote
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any
import unicodedata
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException,
    StaleElementReferenceException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('booking_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ScrapingConfig:
    """Configuration class for scraping parameters"""
    cities: List[str]
    delay_range: tuple = (3, 7)
    max_retries: int = 3
    batch_size: int = 50
    timeout: int = 10
    output_format: str = 'csv'
    output_filename: str = 'booking_apartments.csv'
    filters: Dict[str, Any] = None
    incremental_save: bool = True

    def __post_init__(self):
        if self.filters is None:
            self.filters = {
                'property_types': ['apartment'],
                'min_price': None,
                'max_price': None,
                'min_rating': None
            }

            # Add default dates if not specified
        if not hasattr(self, 'checkin_date'):
            # Default to 30 days from now
            self.checkin_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        if not hasattr(self, 'checkout_date'):
            # Default to 32 days from now (2 night stay)
            self.checkout_date = (datetime.now() + timedelta(days=32)).strftime('%Y-%m-%d')


@dataclass
class ApartmentData:
    """Data class for apartment information"""
    id: int
    title: str
    city: str
    price_per_night: Optional[float]
    address: Optional[str]
    zone: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    bedrooms: Optional[int]
    kitchens: Optional[int]
    surface_m2: Optional[int]
    wifi: bool
    rating: Optional[float]
    review_count: Optional[int]
    amenities: List[str]
    url: str
    scraped_at: str


class DataValidator:
    """Handles data validation and cleaning"""

    @staticmethod
    def clean_latin_text(text: str) -> str:
        """Normalize text by removing non-Latin characters"""
        if not text:
            return text

        text = unicodedata.normalize('NFKD', text)
        pattern = re.compile(r'[^a-zA-Z0-9À-ÖØ-öø-ÿ \-\',.]')
        cleaned = pattern.sub('', text)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    @staticmethod
    def normalize_price(price_text: str) -> Optional[float]:
        """Extract and normalize price from text"""
        if not price_text:
            return None

        price_clean = re.sub(r'[^\d.,]', '', price_text.replace(' ', ''))

        if ',' in price_clean and '.' in price_clean:
            price_clean = price_clean.replace(',', '')
        elif ',' in price_clean:
            price_clean = price_clean.replace(',', '.')

        try:
            return float(price_clean)
        except ValueError:
            return None

    @staticmethod
    def validate_coordinates(lat: str, lng: str) -> tuple:
        """Validate and convert coordinates"""
        try:
            lat_float = float(lat)
            lng_float = float(lng)

            if -90 <= lat_float <= 90 and -180 <= lng_float <= 180:
                return lat_float, lng_float
        except (ValueError, TypeError):
            pass

        return None, None

    @staticmethod
    def validate_apartment_data(data: Dict[str, Any]) -> bool:
        """Validate apartment data quality"""
        if not data.get('title'):
            return False

        required_fields = ['price_per_night', 'address', 'zone']
        if not any(data.get(field) for field in required_fields):
            return False

        return True


class GeocodeService:
    """Handles reverse geocoding using Nominatim API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BookingScraper/2.0 (Educational Purpose)'
        })

    def reverse_geocode(self, lat: float, lng: float) -> Optional[str]:
        """Convert coordinates to address"""
        url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            'format': 'jsonv2',
            'lat': lat,
            'lon': lng
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get("display_name")
        except Exception as e:
            logger.error(f"Geocoding error: {e}")

        return None

    @staticmethod
    def extract_zone(address: str) -> str:
        """Extract district and neighborhood from address"""
        if not address:
            return "Unknown"

        parts = [p.strip() for p in address.split(",")]
        arrondissement = None
        quartier = None

        for part in parts:
            if "Arrondissement" in part or "District" in part:
                arrondissement = part
                break

        if arrondissement:
            index = parts.index(arrondissement)
            if index > 0:
                quartier = parts[index - 1]

        if arrondissement and quartier:
            return f"{quartier.strip()} - {arrondissement.strip()}"
        elif arrondissement:
            return arrondissement.strip()
        else:
            return "Unknown"


class PropertyClassifier:
    """Classifies properties to filter apartments"""

    EXCLUDED_KEYWORDS = [
        'hotel', 'hôtel', 'riad', 'villa', 'palace', 'resort', 'lodge',
        'hostel', 'auberge', 'maison d\'hôtes', 'guest house', 'boutique',
        'spa', 'club', 'camping', 'dar ', 'atelier', 'pension'
    ]

    APARTMENT_INDICATORS = [
        'apartment', 'appartement', 'studio', 'flat', 'appart',
        'logement', 'residence', 'résidence', 'suite', 'loft'
    ]

    @classmethod
    def is_apartment(cls, title: str, page_source: str = "") -> bool:
        """Determine if property is an apartment"""
        if not title:
            return False

        title_lower = title.lower()

        for keyword in cls.EXCLUDED_KEYWORDS:
            if keyword in title_lower:
                return False

        for indicator in cls.APARTMENT_INDICATORS:
            if indicator in title_lower:
                return True

        if page_source:
            page_lower = page_source.lower()
            for indicator in cls.APARTMENT_INDICATORS:
                if indicator in page_lower:
                    return True

        return False


class BookingApartmentScraper:
    """Main scraper class for Booking.com apartments"""

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.driver = None
        self.geocoder = GeocodeService()
        self.wait = None
        self.current_id = 1

    def _save_to_csv_incremental(self, results_batch: List[ApartmentData], filepath: Path):
        """Save a batch of results to CSV, appending if the file exists."""
        try:
            file_exists = filepath.exists()
            with open(filepath, 'a', newline='', encoding='utf-8') as f:
                if results_batch:
                    fieldnames = list(asdict(results_batch[0]).keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)

                    if not file_exists:
                        writer.writeheader()

                    for result in results_batch:
                        row = asdict(result)
                        row['amenities'] = ', '.join(row['amenities'])
                        writer.writerow(row)

            logger.info(f"Appended {len(results_batch)} new results to {filepath}")
        except Exception as e:
            logger.error(f"Error saving incremental CSV: {e}")

    def _setup_driver(self) -> webdriver.Chrome:
        """Initialize Chrome driver with enhanced stealth options"""
        options = webdriver.ChromeOptions()

        # Enhanced stealth options
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")  # Speed up loading
        options.add_argument("--disable-javascript")  # Can be removed if JS is needed
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--ignore-ssl-errors")
        options.add_argument("--ignore-certificate-errors-spki-list")

        # Window size for better compatibility
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")

        # Remove automation indicators
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Enhanced user agent rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        options.add_argument(f"--user-agent={random.choice(user_agents)}")

        # Additional preferences to avoid detection
        prefs = {
            "profile.default_content_setting_values": {
                "images": 2,  # Block images
                "notifications": 2,
                "geolocation": 2,
                "media_stream": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2
            }
        }
        options.add_experimental_option("prefs", prefs)

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )

            # Execute script to hide webdriver property
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_cdp_cmd('Runtime.evaluate', {
                "expression": "Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})"
            })
            driver.execute_cdp_cmd('Runtime.evaluate', {
                "expression": "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})"
            })

            self.wait = WebDriverWait(driver, self.config.timeout)
            logger.info("Driver setup completed successfully")
            return driver
        except Exception as e:
            logger.error(f"Failed to setup driver: {e}")
            raise

    def _random_delay(self, min_sec: int = None, max_sec: int = None):
        """Add random delay to avoid detection"""
        min_sec = min_sec or self.config.delay_range[0]
        max_sec = max_sec or self.config.delay_range[1]
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)

    def _retry_operation(self, func, *args, **kwargs):
        """Retry operation with exponential backoff"""
        for attempt in range(self.config.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise e
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)

    def _build_search_url(self, city: str, offset: int = 0) -> str:
        """Build filtered search URL with pagination support (offset)"""
        city_encoded = quote(city)

        # Removed fixed dates to get all available properties
        base_url = "https://www.booking.com/searchresults.html"
        params = (
            f"?ss={city_encoded}"
            f"&checkin={self.config.checkin_date}"
            f"&checkout={self.config.checkout_date}"
            f"&group_adults=2&no_rooms=1&group_children=0"
            f"&rows=25"  # 25 results per page
            f"&offset={offset}"  # Offset for pagination
            f"&order=popularity"
        )
        return base_url + params

    def _wait_for_page_load(self, timeout=30):
        """Wait for page to fully load with multiple strategies"""
        try:
            # Wait for the page to be ready
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

            # Wait for listings to appear
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, "//div[@data-testid='property-card']"))
            )

            logger.info("Page loaded successfully")
            return True
        except TimeoutException:
            logger.error("Page failed to load within timeout")
            return False

    def _handle_popups_and_cookies(self):
        """Handle common popups and cookie banners"""
        try:
            # Handle cookie consent
            cookie_selectors = [
                "//button[contains(text(), 'Accept') or contains(text(), 'Accepter')]",
                "//button[@id='onetrust-accept-btn-handler']",
                "//button[contains(@class, 'cookie') and contains(text(), 'Accept')]"
            ]

            for selector in cookie_selectors:
                try:
                    element = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    element.click()
                    logger.info("Accepted cookies")
                    time.sleep(2)
                    break
                except:
                    continue

            # Handle location popup
            location_selectors = [
                "//button[contains(text(), 'Not now') or contains(text(), 'Pas maintenant')]",
                "//button[contains(@aria-label, 'Dismiss') or contains(@aria-label, 'Close')]"
            ]

            for selector in location_selectors:
                try:
                    element = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    element.click()
                    logger.info("Dismissed location popup")
                    time.sleep(1)
                    break
                except:
                    continue

            # --- NEW: Handle currency pop-up with your class selector ---
            currency_selectors = [
                "//button[contains(text(),'Non, merci') or contains(text(),'No, thanks')]",
                "//button[contains(text(),'Rester sur le site') or contains(text(),'Stay on site')]",
                "//div[contains(@class,'bui-modal__header')]//button[contains(@class,'close')]",
                "//button[contains(@class, 'de576f5064')]",  # Votre sélecteur de classe
                "//div[contains(@class,'bui-modal__footer')]//button[contains(text(), 'fermer') or contains(text(), 'close')]"
            ]

            for selector in currency_selectors:
                try:
                    button = WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, selector)))
                    if button.is_displayed():
                        self.driver.execute_script("arguments[0].click();", button)
                        logger.info(f"Closed currency pop-up using selector: {selector}")
                        self._random_delay(1, 2)
                        break
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                    continue
            # ----------------------------------------------

        except Exception as e:
            logger.debug(f"No popups to handle: {e}")

    def _load_all_listings(self, city: str) -> List[str]:
        """Load all listings for a city using offset pagination + scrolling"""
        logger.info(f"Loading listings for {city} with offset pagination...")
        all_links = set()
        offset = 0
        max_pages = 50  # safeguard to avoid infinite loop

        while offset < max_pages * 25:
            search_url = self._build_search_url(city, offset)
            self.driver.get(search_url)
            self._random_delay(3, 5)
            self._handle_popups_and_cookies()

            # Wait for listings to load
            if not self._wait_for_page_load():
                logger.warning(f"No listings loaded at offset {offset}")
                break

            # Scroll to ensure lazy-loaded listings appear
            for _ in range(3):
                self.driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
                time.sleep(1)

            # Collect property links
            current_elements = self.driver.find_elements(By.XPATH, "//a[@data-testid='title-link']")
            page_links = {elem.get_attribute("href").split("?")[0] for elem in current_elements if
                          elem.get_attribute("href")}
            logger.info(f"Offset {offset}: found {len(page_links)} links")

            # Stop if no new listings
            before_count = len(all_links)
            all_links.update(page_links)
            if len(all_links) == before_count:
                logger.info("No new listings found, stopping pagination.")
                break

            offset += 25  # move to next page
            self._random_delay(5, 8)

        logger.info(f"Total unique listings collected for {city}: {len(all_links)}")
        return list(all_links)

    def scrape_city(self, city: str) -> List[ApartmentData]:
        """Scrape all apartments for a specific city with improved error handling"""
        logger.info(f"Starting scraping for {city}")

        try:
            # Build search URL
            search_url = self._build_search_url(city)
            logger.info(f"Navigating to: {search_url}")

            # Navigate to search page
            self.driver.get(search_url)
            self._random_delay(5, 8)

            # Handle popups and cookies
            self._handle_popups_and_cookies()

            # Wait for page to load
            if not self._wait_for_page_load():
                logger.error(f"Failed to load search results for {city}")
                return []

            # Load all listings
            all_links = self._load_all_listings(city)
            logger.info(f"Found {len(all_links)} unique listings for {city}")

            if not all_links:
                logger.warning(f"No listings found for {city}!")
                return []

            # Process listings in batches
            batch_results = []
            batch_size = self.config.batch_size

            for i in range(0, len(all_links), batch_size):
                batch = all_links[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(all_links) - 1) // batch_size + 1
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} listings)")

                batch_results = []

                for j, url in enumerate(batch):
                    try:
                        logger.info(f"Scraping listing {i + j + 1}/{len(all_links)}: {url}")
                        apartment = self._retry_operation(self._scrape_single_listing, url)
                        if apartment:
                            apartment.city = city
                            batch_results.append(apartment)
                            logger.info(f"✓ Scraped: {apartment.title} | Price: {apartment.price_per_night}")
                        else:
                            logger.info(f"✗ Skipped: Not an apartment or failed validation")
                    except Exception as e:
                        logger.error(f"Failed to scrape {url}: {e}")
                        continue

                    self._random_delay()

                # Save batch incrementally
                if self.config.incremental_save and batch_results:
                    output_path = Path(self.config.output_filename)
                    self._save_to_csv_incremental(batch_results, output_path)

                # Rest between batches
                if i + batch_size < len(all_links):
                    logger.info("Resting between batches...")
                    self._random_delay(15, 30)

            logger.info(f"Completed {city}: scraped {len(all_links)} total listings")
            return []

        except Exception as e:
            logger.error(f"Error scraping city {city}: {e}")
            return []

    def _scrape_single_listing(self, url: str) -> Optional[ApartmentData]:
        """Scrape individual apartment listing with better error handling"""
        try:
            self.driver.get(url)
            self._random_delay(3, 5)

            # Wait for page to load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
            except TimeoutException:
                logger.warning(f"Page failed to load: {url}")
                return None

            page_source = self.driver.page_source

            # Extract title with multiple strategies
            title = None
            title_selectors = [
                "//h1",
                "//h2[@data-testid='header-title']",
                "//div[@data-testid='property-header']//h1",
                "//h1[contains(@class, 'property-title')]"
            ]

            for selector in title_selectors:
                try:
                    title_element = self.driver.find_element(By.XPATH, selector)
                    title = title_element.text.strip()
                    if title:
                        break
                except:
                    continue

            if not title:
                logger.warning(f"Could not extract title from {url}")
                return None

            if not PropertyClassifier.is_apartment(title, page_source):
                logger.debug(f"Skipping non-apartment property: {title}")
                return None

            # Extract other data...
            price = self._extract_price(page_source)
            address = self._extract_address()
            lat, lng = self._extract_coordinates(page_source)

            if not address and lat and lng:
                address = self.geocoder.reverse_geocode(lat, lng)

            zone = GeocodeService.extract_zone(address)
            details = self._extract_apartment_details(page_source)
            rating, review_count = self._extract_rating_info()

            apartment_data = ApartmentData(
                id=self.current_id,
                title=DataValidator.clean_latin_text(title),
                city="",
                price_per_night=price,
                address=DataValidator.clean_latin_text(address),
                zone=DataValidator.clean_latin_text(zone),
                latitude=lat,
                longitude=lng,
                bedrooms=details['bedrooms'],
                kitchens=details['kitchens'],
                surface_m2=details['surface_m2'],
                wifi=details['wifi'],
                rating=rating,
                review_count=review_count,
                amenities=details['amenities'],
                url=url,
                scraped_at=datetime.now().isoformat()
            )

            if DataValidator.validate_apartment_data(asdict(apartment_data)):
                self.current_id += 1
                return apartment_data
            else:
                logger.debug(f"Data validation failed for: {title}")
                return None

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None

    def _extract_address(self) -> Optional[str]:
        """Extract address with multiple strategies"""
        address_selectors = [
            "//span[@data-node_tt_id='location_score_tooltip']",
            "//div[contains(@class, 'hp_address_subtitle')]",
            "//span[contains(@class, 'hp_address_subtitle')]",
            "//*[@data-testid='address']",
            "//p[contains(@class, 'address')]"
        ]

        for selector in address_selectors:
            try:
                element = self.driver.find_element(By.XPATH, selector)
                address = element.text.strip()
                if address:
                    return address
            except:
                continue
        return None

    def _extract_coordinates(self, page_source: str) -> tuple:
        """Extract coordinates from page source"""
        patterns = [
            r'"latitude":([0-9\.\-]+),"longitude":([0-9\.\-]+)',
            r'"lat":([0-9\.\-]+),"lng":([0-9\.\-]+)',
            r'latitude.*?([0-9\.\-]+).*?longitude.*?([0-9\.\-]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, page_source)
            if match:
                return DataValidator.validate_coordinates(match.group(1), match.group(2))

        return None, None

    def _extract_price(self, page_source: str) -> Optional[float]:
        """Extract price using multiple strategies"""
        strategies = [
            lambda: self._get_price_from_elements(),
            lambda: self._get_price_from_source(page_source)
        ]

        for strategy in strategies:
            try:
                price = strategy()
                if price:
                    return DataValidator.normalize_price(str(price))
            except Exception:
                continue

        return None

    def _get_price_from_elements(self) -> Optional[str]:
        """Extract price from DOM elements"""
        selectors = [
            "//span[contains(@aria-label, 'Prix') or contains(@aria-label, 'Price')]",
            "//div[@data-testid='price-and-discounted-price']//span",
            "//span[@data-testid='price-and-discounted-price']",
            "//div[contains(@class, 'bui-price-display__value')]//span",
            "//span[contains(text(), 'MAD') or contains(text(), '€') or contains(text(), '$')]",
            "//*[contains(@class, 'prco-valign-middle-helper')]//span"
        ]

        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and re.search(r'\d+', text):
                        return text
            except Exception:
                continue

        return None

    def _get_price_from_source(self, source: str) -> Optional[str]:
        """Extract price from page source using regex"""
        patterns = [
            r'"price":\s*"?(\d+(?:[.,]\d+)?)"?',
            r'"totalPrice":\s*"?(\d+(?:[.,]\d+)?)"?',
            r'"displayPrice":\s*"?(\d+(?:[.,]\d+)?)"?',
            r'MAD\s*(\d+(?:[.,]\d+)*)',
            r'€\s*(\d+(?:[.,]\d+)*)',
            r'(\d+(?:[.,]\d+)*)\s*MAD',
            r'(\d+(?:[.,]\d+)*)\s*€'
        ]

        for pattern in patterns:
            match = re.search(pattern, source)
            if match:
                return match.group(1)

        return None

    def _extract_apartment_details(self, page_source: str) -> Dict[str, Any]:
        """Extract detailed apartment information"""
        details = {
            'bedrooms': None,
            'kitchens': None,
            'surface_m2': None,
            'wifi': False,
            'amenities': []
        }

        try:
            # Extract bedrooms
            bedroom_patterns = [
                r'(\d+)\s*chambre',
                r'(\d+)\s*bedroom',
                r'(\d+)\s*bed(?!room)',
                r'chambre\s*:\s*(\d+)',
                r'bedroom\s*:\s*(\d+)'
            ]

            for pattern in bedroom_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    details['bedrooms'] = int(match.group(1))
                    break

            # Extract kitchens
            kitchen_patterns = [
                r'(\d+)\s*cuisine',
                r'(\d+)\s*kitchen',
                r'cuisine\s*:\s*(\d+)',
                r'kitchen\s*:\s*(\d+)'
            ]

            for pattern in kitchen_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    details['kitchens'] = int(match.group(1))
                    break

            if details['kitchens'] is None:
                if re.search(r'cuisine|kitchen|kitchenette', page_source, re.IGNORECASE):
                    details['kitchens'] = 1

            # Extract surface area
            surface_patterns = [
                r'(\d+)\s*m²',
                r'(\d+)\s*m2',
                r'(\d+)\s*square\s*meter',
                r'(\d+)\s*sqm'
            ]

            for pattern in surface_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    details['surface_m2'] = int(match.group(1))
                    break

            # Check for WiFi
            wifi_indicators = ['wifi', 'wi-fi', 'wireless', 'internet']
            for indicator in wifi_indicators:
                if re.search(indicator, page_source, re.IGNORECASE):
                    details['wifi'] = True
                    break

            # Extract amenities
            amenity_patterns = [
                r'air conditioning|climatisation',
                r'parking|garage',
                r'balcony|balcon|terrasse',
                r'pool|piscine',
                r'gym|fitness|salle de sport'
            ]

            for pattern in amenity_patterns:
                if re.search(pattern, page_source, re.IGNORECASE):
                    details['amenities'].append(pattern.split('|')[0])

        except Exception as e:
            logger.error(f"Error extracting apartment details: {e}")

        return details

    def _extract_rating_info(self) -> tuple:
        """Extract rating and review count"""
        rating, review_count = None, None

        try:
            # Rating selectors
            rating_selectors = [
                "//div[@data-testid='review-score-component']/div[1]",
                "//div[contains(@class, 'bui-review-score__badge')]",
                "//span[contains(@class, 'review-score-badge')]",
                "//*[@data-testid='review-score-right-component']//div[1]"
            ]

            for selector in rating_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    text = element.text.strip()
                    match = re.search(r'(\d+(?:[.,]\d+)?)', text)
                    if match:
                        rating = float(match.group(1).replace(',', '.'))
                        break
                except Exception:
                    continue

            # Review count selectors
            review_selectors = [
                "//div[@data-testid='review-score-component']//div[contains(text(), 'commentaire') or contains(text(), 'review')]",
                "//*[contains(text(), 'commentaire') or contains(text(), 'review')]",
                "//*[@data-testid='review-score-right-component']//div[2]"
            ]

            for selector in review_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    text = element.text.strip()
                    match = re.search(r'(\d+)', text)
                    if match:
                        review_count = int(match.group(1))
                        break
                except Exception:
                    continue

        except Exception as e:
            logger.error(f"Error extracting rating: {e}")

        return rating, review_count

    def scrape_all_cities(self) -> List[ApartmentData]:
        """Scrape all configured cities with improved error handling"""
        all_results = []

        try:
            self.driver = self._setup_driver()
            logger.info("Driver initialized successfully")

            for i, city in enumerate(self.config.cities):
                logger.info(f"Starting city {i + 1}/{len(self.config.cities)}: {city}")

                try:
                    city_results = self.scrape_city(city)
                    all_results.extend(city_results)
                except Exception as e:
                    logger.error(f"Failed to scrape city {city}: {e}")
                    continue

                # Rest between cities
                if i < len(self.config.cities) - 1:
                    logger.info("Resting between cities...")
                    self._random_delay(30, 60)

        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {e}")
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("Driver closed successfully")
                except Exception as e:
                    logger.error(f"Error closing driver: {e}")

        return all_results

    def _save_to_csv(self, results: List[ApartmentData], filepath: Path):
        """Save results to CSV file"""
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                if results:
                    fieldnames = list(asdict(results[0]).keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    for result in results:
                        row = asdict(result)
                        row['amenities'] = ', '.join(row['amenities'])
                        writer.writerow(row)

            logger.info(f"Results saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    def _save_to_json(self, results: List[ApartmentData], filepath: Path):
        """Save results to JSON file"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump([asdict(result) for result in results], f, ensure_ascii=False, indent=2)

            logger.info(f"Results saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")

    def print_summary(self, results: List[ApartmentData]):
        """Print scraping summary"""
        if not results:
            logger.info("No data was scraped!")
            return

        logger.info(f"\n=== SCRAPING COMPLETE ===")
        logger.info(f"Total properties scraped: {len(results)}")

        # City breakdown
        city_counts = {}
        for result in results:
            city_counts[result.city] = city_counts.get(result.city, 0) + 1

        logger.info("\nCity breakdown:")
        for city, count in city_counts.items():
            logger.info(f"  {city}: {count} properties")

        # Data quality metrics
        with_price = sum(1 for r in results if r.price_per_night)
        with_wifi = sum(1 for r in results if r.wifi)
        with_bedrooms = sum(1 for r in results if r.bedrooms)
        with_rating = sum(1 for r in results if r.rating)

        logger.info(f"\nData quality metrics:")
        logger.info(f"  Properties with price: {with_price}")
        logger.info(f"  Properties with WiFi: {with_wifi}")
        logger.info(f"  Properties with bedroom info: {with_bedrooms}")
        logger.info(f"  Properties with ratings: {with_rating}")


def main():
    """Main function to run the scraper with better error handling"""
    try:
        # Configuration
        config = ScrapingConfig(
            cities=["Marrakech"],
            delay_range=(5, 10),
            max_retries=5,
            batch_size=10,
            timeout=20,
            output_format='csv',
            output_filename='booking_apartments_improved.csv',
            incremental_save=True
        )
        config.checkin_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        config.checkout_date = (datetime.now() + timedelta(days=32)).strftime('%Y-%m-%d')

        logger.info("=== Enhanced Booking.com Apartment Scraper ===")
        logger.info(f"Cities to scrape: {', '.join(config.cities)}")
        logger.info(f"Batch size: {config.batch_size}")
        logger.info(f"Incremental save: {config.incremental_save}")

        # Initialize and run scraper
        scraper = BookingApartmentScraper(config)
        scraper.scrape_all_cities()

        logger.info("Scraping completed successfully!")

    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise


if __name__ == "__main__":
    main()