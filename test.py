import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import os
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def test_selenium_connection_with_proxy():
    """Teste la connexion à Google.com en utilisant un proxy."""
    print("=== TEST DE CONNEXION SELENIUM AVEC PROXY ===")

    # Définir l'adresse du proxy
    # proxy_address = "MeditelProxy.meditel.int:80"

    # Configure les options de Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # Execute in headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    

    # Add these arguments to handle SSL and privacy errors caused by proxies
    chrome_options.add_argument('--ignore-ssl-errors=true')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    chrome_options.add_argument('--ignore-certificate-errors')

    # Explicitly add the proxy server argument
   # chrome_options.add_argument(f'--proxy-server=http://{proxy_address}')

    # URL of the Selenium Hub, using 'localhost' which resolves to 127.0.0.1
    selenium_url = os.environ.get('SELENIUM_URL', 'http://localhost:4444')

    driver = None
    try:
        print(f"Tentative de connexion à Selenium Hub à l'adresse : {selenium_url}")
        driver = webdriver.Remote(
            command_executor=selenium_url,
            options=chrome_options
        )
        print("Connexion au serveur Selenium établie avec succès.")
        
        print("Navigation vers https://www.google.com...")
        driver.get("https://www.google.com")
        
        # Give the page a moment to load and handle any redirects
        time.sleep(3)
        
        # Get the page title
        title = driver.title
        print(f"Titre de la page : '{title}'")
        
        # The check is now more robust against redirects and errors
        if "Google" in title:
            print("✅ Succès : La page Google.com a été chargée correctement via le proxy.")
        else:
            print(f"❌ Échec : Le titre de la page n'est pas celui attendu. Titre actuel : '{title}'")
            # You can also print the page source for further debugging
            # print("Page source:\n", driver.page_source)

    except WebDriverException as e:
        print(f"❌ Erreur de WebDriver : Impossible de se connecter ou de charger la page. Détails : {e}")
        print("\nCauses possibles :")
        print("- Le proxy est incorrectement configuré ou ne répond pas.")
        print("- Le conteneur Docker de Selenium n'est pas en cours d'exécution.")
        print("- Le pare-feu bloque toujours la connexion.")
        
    finally:
        if driver:
            driver.quit()
            print("Session WebDriver fermée.")

if __name__ == "__main__":
    test_selenium_connection_with_proxy()