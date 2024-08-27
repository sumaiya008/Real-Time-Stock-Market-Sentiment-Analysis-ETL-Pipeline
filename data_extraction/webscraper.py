# Importing libraries
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import asyncio
import aiohttp
import async_timeout
from functools import reduce
from webdriver_manager.chrome import ChromeDriverManager

class AsyncWebScraper:
    """
    AsyncWebScraper encapsulates the logic for asynchronously scraping
    multiple web pages and extracting links or HTML content.
    """

    def __init__(self, urls: list) -> None:
        """
        Initializes the scraper with a list of URLs.

        Args:
            urls (list): List of URLs to scrape.
        """
        self.target_urls = urls
        self.collected_links = {}
        self._lock = asyncio.Lock()

    async def _fetch_links(self, url: str) -> None:
        """
        Uses Selenium with headless Chrome to fetch links from a webpage.

        Args:
            url (str): URL of the webpage to scrape.
        """
        browser = None
        try:
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--headless')
            options.add_argument('--disable-dev-shm-usage')

            browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            browser.get(url)

            soup = BeautifulSoup(browser.page_source, 'lxml')
            links = [link.get('href') for link in soup.find_all('a', href=True)]

            async with self._lock:
                self.collected_links[url] = links

        except Exception as err:
            print(f"Error while fetching links from {url}: {err}")
        finally:
            if browser:
                browser.quit()

    async def _gather_links(self) -> None:
        """
        Gathers links from all target URLs asynchronously.
        """
        tasks = [self._fetch_links(url) for url in self.target_urls]
        await asyncio.gather(*tasks)

    def fetch_all_links(self) -> None:
        """
        Initiates the asynchronous gathering of links.
        """
        asyncio.run(self._gather_links())

    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> tuple:
        """
        Asynchronously fetches the HTML content of a given URL.

        Args:
            session (aiohttp.ClientSession): Active aiohttp session.
            url (str): URL to fetch HTML from.

        Returns:
            tuple: HTML content and the source URL, or an error message and the URL.
        """
        try:
            async with async_timeout.timeout(10):
                async with session.get(url) as response:
                    return await response.text(), url
        except (asyncio.exceptions.TimeoutError, aiohttp.client_exceptions.InvalidURL,
                aiohttp.client_exceptions.ServerDisconnectedError,
                aiohttp.client_exceptions.ClientConnectorError, UnicodeDecodeError) as e:
            return f"Error: {str(e)}", url

    async def _fetch_all_html(self, session: aiohttp.ClientSession, urls: list) -> list:
        """
        Creates tasks to fetch HTML content from multiple URLs asynchronously.

        Args:
            session (aiohttp.ClientSession): Active aiohttp session.
            urls (list): List of URLs to fetch HTML from.

        Returns:
            list: List of tuples containing HTML content and URL.
        """
        tasks = [self._fetch_html(session, url) for url in urls]
        return await asyncio.gather(*tasks)

    async def scrape_html(self, urls: list) -> dict:
        """
        Initiates asynchronous HTML scraping for the given URLs.

        Args:
            urls (list): List of URLs to scrape.

        Returns:
            dict: A dictionary mapping each URL to its HTML content or an error message.
        """
        async with aiohttp.ClientSession() as session:
            html_data = await self._fetch_all_html(session, urls)
            return {url: self._extract_text(html) for html, url in html_data}

    def scrape_and_get_html(self, urls: list) -> dict:
        """
        Wrapper method for scrape_html to run it synchronously.

        Args:
            urls (list): List of URLs to scrape.

        Returns:
            dict: A dictionary mapping each URL to its HTML content or an error message.
        """
        return asyncio.run(self.scrape_html(urls))

    def _extract_text(self, html: str) -> tuple:
        """
        Extracts the main text content and title from HTML.

        Args:
            html (str): The raw HTML content.

        Returns:
            tuple: A tuple containing the extracted text and title.
        """
        soup = BeautifulSoup(html, 'lxml')
        title = soup.title.get_text() if soup.title else "No Title Found"
        for element in soup(['script', 'style', 'template', 'TemplateString', 'ProcessingInstruction', 'Declaration', 'Doctype']):
            element.extract()
        text = ' '.join([p.get_text().strip().replace(u'\xa0', u' ') for p in soup.find_all('p')])
        return text, title

if __name__ == '__main__':
    urls_to_scrape = ['https://finance.yahoo.com/news/', 'https://www.marketwatch.com/latest-news?mod=top_nav']
    scraper = AsyncWebScraper(urls_to_scrape)
    
    # Fetch and display all links
    scraper.fetch_all_links()
    for url, links in scraper.collected_links.items():
        print(f"Links found on {url}:")
        for link in links:
            print(link)
    
    # #Example of scraping HTML content (uncomment if needed)
    # html_data = scraper.scrape_and_get_html(urls_to_scrape)
    # for url, content in html_data.items():
    #     print(f"HTML content for {url}: {content[:500]}...")  # Print first 500 characters of content
