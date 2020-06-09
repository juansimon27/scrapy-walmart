import scrapy
import json
import re
from product_scraping.items import Product
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class CaWalmartBot(scrapy.Spider):
    name = 'ca_walmart'
    allowed_domains = ['walmart.ca']
    start_urls = ['https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852']
    header = {
        'Host': 'www.walmart.ca',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'WM_QOS.CORRELATION_ID': '93421d13-022-1728f3e61789b6,93421d13-022-1728f3e6179db5,93421d13-022-1728f3e6179db5',
        'Content-Type': 'application/json',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    def parse(self, response):
        for url in response.css('.product-link::attr(href)').getall():
            yield response.follow(url, callback=self.parse_html)
        next_page = response.css('#loadmore::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def parse_html(self, response):
        item = Product()
        branches = {'3106': ['43.656422', '-79.435567'], '3124': ['48.412997', '-89.239717']}

        f_selector = json.loads(re.findall(r'(\{.*\})', response.xpath("/html/body/script[1]/text()").get())[0])
        s_selector = json.loads(response.css('.evlleax2 > script:nth-child(1)::text').get())
        url_product = response.xpath('/html/head/link[40]/@href').get()
        sku = s_selector['sku']
        upc = f_selector['entities']['skus'][sku]['upc']
        product_type = f_selector['entities']['skus'][sku]['facets'][0]['value']
        category_1 = f_selector['entities']['skus'][sku]['categories'][0]['hierarchy'][0]['displayName']['en']
        category_2 = f_selector['entities']['skus'][sku]['categories'][0]['hierarchy'][1]['displayName']['en']
        category_3 = f_selector['entities']['skus'][sku]['categories'][0]['hierarchy'][2]['displayName']['en']
        category = ' | '.join([category_3, category_2, category_1, product_type])
        package = f_selector['entities']['skus'][sku]['description']
        description = s_selector['description']
        name = s_selector['name']
        brand = s_selector['brand']['name']
        image_url = s_selector['image']

        item['barcodes'] = ', '.join(upc)
        item['store'] = re.findall(r'\.[\w]+\.', url_product)[0].replace('.', '').capitalize()
        item['category'] = category
        item['package'] = package
        item['url'] = url_product
        item['brand'] = brand
        item['image_url'] = ', '.join(image_url)
        item['description'] = description.replace('<br>', '')
        item['sku'] = sku
        item['name'] = name

        url_json = 'https://www.walmart.ca/api/product-page/find-in-store?' \
                   'latitude={}&longitude={}&lang=en&upc={}'

        for k in branches.keys():
            yield scrapy.http.Request(url_json.format(branches[k][0], branches[k][1], upc[0]),
                                      callback=self.parse_api, cb_kwargs={'item': item},
                                      meta={'handle_httpstatus_all': True},
                                      dont_filter=False, headers=self.header)

    def parse_api(self, response, item):
        json_response = json.loads(response.body)

        branch = json_response['info'][0]['id']
        stock = json_response['info'][0]['availableToSellQty']
        if 'sellPrice' not in json_response['info'][0]:
            price = 0
        else:
            price = json_response['info'][0]['sellPrice']

        item['branch'] = branch
        item['stock'] = stock
        item['price'] = price

        yield item

    def errback_httpbin(self, failure):
        # logs failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("HttpError occurred on %s", response.url)

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error("DNSLookupError occurred on %s", request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error("TimeoutError occurred on %s", request.url)
