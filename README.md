# Web Crawler + Data Ingestion
A python script to obtain product data and store availability from Walmart Canada website for specific categories and branches. It also digests and integrates additional data to the database produced by the scraper.

## Packages
- Scrapy
- Pandas
- SQLAlchemy

## Execution Workflow

- Run **spider.py** within **product_scraping** folder: 
  ```
  spider crawl ca_walmart
  ```
- Run **ingestion.py** within **additional_data** folder:
  ```
  python ingestion.py
  ```
