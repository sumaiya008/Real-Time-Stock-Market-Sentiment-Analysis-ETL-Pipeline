# import time
# from selenium import webdriver

# driver = webdriver.Chrome()  # Optional argument, if not specified will search path.
# driver.get('http://www.google.com/')
# time.sleep(5) # Let the user actually see something!
# search_box = driver.find_element_by_name('q')
# search_box.send_keys('ChromeDriver')
# search_box.submit()
# time.sleep(5) # Let the user actually see something!
# driver.quit()

from webscraper import AsyncWebScraper
from webscrape_flow import yahoo_url_filter, extract_urls_to_news, webscrape_extract
from unittest.mock import patch

# Test the URL filtering function
def test_url_filter():
    urls = ['https://finance.yahoo.com/news/article1',
            'https://finance.yahoo.com/news/article2',
            'https://www.marketwatch.com/story/article3']

    filtered_urls = yahoo_url_filter(urls)
    assert len(filtered_urls) == 2  # Expect only Yahoo Finance URLs

# Test the extract_urls_to_news flow with a mock for fetch_all_links
@patch('webscraper.AsyncWebScraper.fetch_all_links')
def test_extract_urls_to_news(mock_fetch):
    mock_fetch.return_value = None  # Mock fetch_all_links behavior
    scraper = extract_urls_to_news(['https://finance.yahoo.com/news/'])
    assert scraper is not None  # Ensure the scraper object is returned

if __name__ == '__main__':
    # Run the tests
    print("Running test_url_filter...")
    test_url_filter()
    print("test_url_filter passed.")

    print("Running test_extract_urls_to_news...")
    test_extract_urls_to_news()
    print("test_extract_urls_to_news passed.")

    # Run the main flow
    print("Running webscrape_extract flow...")
    webscrape_extract()  # This runs your main flow
    print("webscrape_extract flow completed.")
