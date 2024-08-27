# Importing libraries
import sys
import os
from prefect import flow, task
from webscraper import AsyncWebScraper
import config
import asyncio
from prefect_dask import DaskTaskRunner, get_dask_client
import dask 
import dask.distributed
from data_loading.s3_io_manager import S3IOManager

import datetime 

# Initialize Dask client for parallelization of flows
client = dask.distributed.Client()

def url_filter(url: str, block_set: set) -> bool:
    """
    Filter function to remove unwanted URLs from results.

    Args:
        url (str): URL string
        block_set (set): Set of unwanted URL strings

    Returns:
        bool: True if URL is acceptable, False otherwise
    """
    return url not in block_set

def extract_news_text(scraper: AsyncWebScraper, website: str) -> dict:
    """
    Extract raw text data from news links from a defined website.

    Args:
        scraper (AsyncWebScraper): Scraper object containing the URLs to be scraped
        website (str): Defined top-level website

    Returns:
        dict: Dictionary mapping URLs to their text content and titles
    """
    # Perform async requests of news articles, returns tuples of (raw HTML data, URL source)
    link_to_data = scraper.scrape_and_get_html(scraper.collected_links[website])
    return link_to_data

def yahoo_url_filter(urls: list) -> list:
    """
    Perform URL filtering and data validation for Yahoo Finance News.

    Args:
        urls (list): List of URLs

    Returns:
        list: Formatted and cleaned list of URLs to request data from
    """
    # Filter unwanted links
    filtered_urls = list(filter(lambda url: url_filter(url, config.yahoo_fin_block_set), urls))
    # Perform data validation on URLs to ensure they are complete URLs
    for i in range(len(filtered_urls)):
        if not filtered_urls[i].startswith('https'):
            filtered_urls[i] = 'https://finance.yahoo.com' + filtered_urls[i]
    return filtered_urls

def marketwatch_url_filter(urls: list) -> list:
    """
    Perform URL filtering and data validation for MarketWatch News.

    Args:
        urls (list): List of URLs

    Returns:
        list: Formatted and cleaned list of URLs to request data from
    """
    # Filter unwanted links
    filtered_urls = list(filter(lambda url: url.startswith('https://www.marketwatch.com/story'), urls))
    return filtered_urls

@task 
def extract_yahoo_finance_news(scraper: AsyncWebScraper, website: str) -> dict:
    """
    Extract Yahoo Finance News.

    Args:
        scraper (AsyncWebScraper): Scraper object containing the URLs to be scraped
        website (str): Top-level website string

    Returns:
        dict: Dictionary mapping URLs to their text content and titles
    """
    # Perform filter of unwanted links and append domain if needed
    filtered_urls = yahoo_url_filter(scraper.collected_links[website])
    # Update Yahoo Finance key
    scraper.collected_links[website] = filtered_urls
    # Extract data
    link_to_data = extract_news_text(scraper, website)
    return link_to_data

@task 
def extract_marketwatch_news(scraper: AsyncWebScraper, website: str) -> dict:
    """
    Extract MarketWatch News.

    Args:
        scraper (AsyncWebScraper): Scraper object containing the URLs to be scraped
        website (str): Top-level website string

    Returns:
        dict: Dictionary mapping URLs to their text content and titles
    """
    # Perform filter of unwanted links and append domain if needed
    filtered_urls = marketwatch_url_filter(scraper.collected_links[website])
    # Update MarketWatch key
    scraper.collected_links[website] = filtered_urls
    # Extract data
    link_to_data = extract_news_text(scraper, website)
    return link_to_data

@flow 
def extract_urls_to_news(urls: list) -> AsyncWebScraper:
    """
    Extract links to news articles from top-level news source websites.

    Args:
        urls (list): List of top-level news sources

    Returns:
        AsyncWebScraper: Scraper object containing the URLs to be scraped and implementation to scrape websites
    """
    # Define scraper object with the list of URLs
    scraper = AsyncWebScraper(urls)
    # Retrieve all links from top-level URL
    scraper.fetch_all_links()
    # Return the scraper object
    return scraper

@flow(task_runner=DaskTaskRunner(address=client.scheduler.address))
def extract_news(scraper: AsyncWebScraper, websites: list) -> None:
    """
    Extract text information from news links existing on the top-level websites given.

    Args:
        scraper (AsyncWebScraper): Scraper object that contains async methods and URL info
        websites (list): List of top-level websites to scrape

    Returns:
        list: List of tuples containing (text data, title, URL source)
    """
    # PrefectFuture Object
    future_yahoo = extract_yahoo_finance_news.submit(scraper, websites[0])
    future_marketwatch = extract_marketwatch_news.submit(scraper, websites[1])
    
    # Return Dask client 
    with get_dask_client():
        # Upload web scraper implementation to Dask workers
        client.upload_file('/opt/sentiment_analysis_pipeline/prefect_flows/implementations/extract_webscrape/webscraper.py')
        # Retrieve data 
        yahoo_data = future_yahoo.result()
        marketwatch_data = future_marketwatch.result()
    
    return [('finance_yahoo_news', yahoo_data), ('marketwatch_latest_news', marketwatch_data)]

@flow 
def push_to_s3(bucket_block: str, data: list) -> None:
    """
    Push data to S3 Bucket.

    Args:
        bucket_block (str): S3 bucket block identifier
        data (list): List of tuples (website name, dict of data)
    """
    # Define IO object 
    i_o = io(bucket_block)
    # Define timestamp for file name 
    t_stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # Write data to JSON in temp dir 
    for website, d in data:
        i_o.write_to_json(d, f'{t_stamp}_{website}')
    # Push data to S3 bucket 
    i_o.pushObject_to_S3()
    # Clean up IO object 
    del i_o
    return 

@flow
def webscrape_extract() -> None:
    """
    Main extract entry flow to obtain HTML data from defined top-level finance news sources.
    """
    # Define list of finance websites to scrape 
    websites = config.top_level_websites
    # Kickoff extract_urls_to_news links flow
    scraper = extract_urls_to_news(websites)
    # Extract text data from the defined websites 
    website_datalist = extract_news(scraper, websites)
    # Push data into cloud storage 
    push_to_s3(config.s3_block, website_datalist)
    return 

if __name__ == '__main__':
    # Kickoff web scrape extract flow 
    webscrape_extract()
