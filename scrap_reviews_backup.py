import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from collections import defaultdict


driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

url = "https://www.booking.com/hotel/ma/appartement-patio-guilez.html?aid=304142&label=gen173nr-10CAsojAFCC3JpYWQtYWJqYW91SDNYBGiMAYgBAZgBM7gBF8gBDNgBA-gBAfgBAYgCAagCAbgCp9LxxAbAAgHSAiQ0ZGZjZTA2YS00MmYzLTQ4MTctYjJkNi1hOTQzNjI0NWJkNDnYAgHgAgE&sid=9143c5c5bfd2d34148964cb1aad92b46&all_sr_blocks=577091001_223838430_2_0_0&checkin=2025-08-13&checkout=2025-08-14&dest_id=-38833&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=9&highlighted_blocks=577091001_223838430_2_0_0&hpos=9&matching_block_id=577091001_223838430_2_0_0&nflt=sth%3D1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&sr_pri_blocks=577091001_223838430_2_0_0__5000&srepoch=1755084992&srpvid=689c78bb012a0087c1257ddf9f7281b7&type=total&ucfs=1&"
driver.get(url)
time.sleep(2)

try:
    # Click reviews button
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "js--hp-gallery-scorecard"))
    ).click()
    print("Button clicked")

    # Get the review score (use textContent for hidden elements)
    comfort_score = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[4]')
        )
    )
    print("comfort:", comfort_score.get_attribute("textContent"))
    value_of_money_score = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[5]')
        )
    )
    print("value of money:", value_of_money_score.get_attribute("textContent"))
    location_score = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, '(//div[@data-testid="review-subscore"]//div[@aria-hidden="true"])[6]')
        )
    )
    print("Location:", location_score.get_attribute("textContent"))

    general_score = WebDriverWait(driver,10).until(
        EC.presence_of_element_located(
            (By.XPATH, '//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div[2]')
        )
    )

    print("score generale:", general_score.get_attribute("textContent"))

    select = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name="customerType"]'))
    )
    options = {opt.text: opt.get_attribute('value')
               for opt in select.find_elements(By.TAG_NAME, 'option')}
    print("Options client:", options)

    # Créer un objet Select
    select_element = Select(select)

    select_element.select_by_value("COUPLES")
    print("Option 'Couples' sélectionnée")
    time.sleep(2)

    first_review_score = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.XPATH, '//*[@id="reviewCardsSection"]/div[1]/div[1]/div/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/div[2]')
        )
    )
    print("first_review_score:", first_review_score.get_attribute("textContent"))

    # Wait for reviews to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="review-card"]'))
    )

    # Find all review cards
    review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-testid="review-card"]')

    # # Loop through each review card
    # for index, card in enumerate(review_cards, 1):
    #     try:
    #         # Get score within the current review card
    #         score = card.find_element(
    #             By.XPATH, './/div[contains(@class, "bc946a29db")]'
    #         ).text.replace('Scored ', '').strip()
    #
    #         print(f"Review {index} score:", score)
    #
    #     except Exception as e:
    #         print(f"Couldn't get score for review {index}: {str(e)}")
    #
    # # Click next reviews button
    # WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, '//*[@id="reviewCardsSection"]/div[2]/div[1]/div/div/div[3]/button'))
    # ).click()
    # print("Button clicked")
    #
    # time.sleep(20)


    # paginating through all reviews
    category_scores = defaultdict(list) # To store scores by category
    while True:
        try:
            # Wait and scrape current page reviews
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="review-card"]'))
            )
            review_cards = driver.find_elements(By.CSS_SELECTOR, '[data-testid="review-card"]')

            # Process reviews on current page
            for index, card in enumerate(review_cards, 1):
                try:
                    # Extract score
                    score_element = card.find_element(
                        By.XPATH, './/div[contains(@class, "bc946a29db")]'
                    )
                    score = float(score_element.text.replace('Scored ', '').strip())

                    # Extract traveler type with better error handling
                    traveler_type = "Unknown"
                    try:
                        traveler_type_element = card.find_element(
                            By.CSS_SELECTOR, '[data-testid="review-traveler-type"]'
                        )
                        traveler_type = traveler_type_element.text.strip() or "Unknown"
                    except Exception as e:
                        print(f"Couldn't find traveler type for review {index}: {e}")

                    # Store data
                    category_scores[traveler_type].append(score)

                    # Print individual review
                    print(f"Review {index} - Category: {traveler_type}, Score: {score}")

                except Exception as e:
                    print(f"Error processing review {index}: {str(e)}")

            # Pagination control
            next_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="reviewCardsSection"]/div[2]/div[1]/div/div/div[3]/button')
                )
            )

            if "disabled" in next_btn.get_attribute("class"):
                print("\n=== Final Results ===")
                for category, scores in sorted(category_scores.items()):
                    avg = sum(scores) / len(scores)
                    print(f"{category}: {avg:.2f} (from {len(scores)} reviews)")
                print("Reached last page")
                break

            next_btn.click()
            print("\nLoading next page...")
            time.sleep(1.5)  # Slightly reduced wait time

        except Exception as e:
            print(f"Critical pagination error: {e}")
            if category_scores:
                print("\n=== Partial Results ===")
                for category, scores in sorted(category_scores.items()):
                    avg = sum(scores) / len(scores)
                    print(f"{category}: {avg:.2f} (from {len(scores)} reviews)")
            break

except Exception as e:
    print("Error:", e)

