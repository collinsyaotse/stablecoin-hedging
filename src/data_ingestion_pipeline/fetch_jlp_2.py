from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # optional: run without opening browser window
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Point to your ChromeDriver path
service = Service('/usr/local/bin/chromedriver')

# Launch browser
driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=chrome_options)


# Example: open a DEX Screener page
url = "https://dexscreener.com/solana/g7ixpyiyneggvf1vansetfmnbvuvcptimjmd9axfqqng"
driver.get(url)

# Wait for JavaScript to render (adjust timing if needed)
time.sleep(5)

# Parse page
soup = BeautifulSoup(driver.page_source, "html.parser")

# Save the HTML to file (optional)
with open("dexscreener_page.html", "w", encoding='utf-8') as f:
    f.write(soup.prettify())

# Close driver
driver.quit()
