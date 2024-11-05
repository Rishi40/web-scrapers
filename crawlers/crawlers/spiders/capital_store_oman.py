from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log, get_size_from_title
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 57
PRODUCT_LIST_LIMIT = 36

CATALOG_HEADERS = {
    'accept': 'text/html, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_capital_store_oman"
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.CrawlerPipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category

        url = f'https://capitalstoreoman.com/collections/{self.sub_category}?page=1'
        catalog_url = SCRAPER_URL + url
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        yield scrapy.Request(
            url=catalog_url, 
            headers=CATALOG_HEADERS,
            callback=self.parse_catalogue_pages,
            meta = {'visited_sku_list':visited_sku_list},
            dont_filter=True
        )

        # For Testing
        # url = f'https://capitalstoreoman.com/products/myclarins-pure-reset-purifying-and-matifying-toner-200ml'
        # catalog_url = SCRAPER_URL + url
        # metadata = {}
        # metadata['product_url'] = url

        # yield scrapy.Request(
        #     url=catalog_url, 
        #     callback=self.parse_product, 
        #     meta={'metadata':metadata}
        # )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WdcSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        stats = spider.crawler.stats.get_stats()
        spider.crawler.stats.set_value('sub_category', self.sub_category)
        log_file_name = f"{spider.name}_log.txt"
        write_to_log(log_file_name,spider.name,stats)

    def parse_catalogue_pages(self, response):
        visited_sku_list = response.meta['visited_sku_list']

        try:
            product_count_string = response.css('div.results-count.results-count--lower::text').get()
            product_count = product_count_string.split()[0]
            no_of_pages = int(product_count) // PRODUCT_LIST_LIMIT + 1
            print("NO OF PAGES ====", no_of_pages)

            # no_of_pages = 1 ## For Testing
            for page_index in range(1,no_of_pages+1):
                url = f'https://capitalstoreoman.com/collections/{self.sub_category}?page={page_index}'
                catalog_url = SCRAPER_URL + url

                yield scrapy.Request(
                    url=catalog_url, 
                    headers=CATALOG_HEADERS,
                    callback=self.parse_catalogue_links,
                    meta = {'visited_sku_list':visited_sku_list},
                    dont_filter=True
                )

        except Exception as e:
            print("=====ERROR IN FETCHING PAGE NUMBER==========")

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        try:
            product_cards_string = response.xpath("//script[contains(.,'ItemList')]/text()")[0].extract()
            product_cards = json.loads(product_cards_string)
            products = product_cards.get('itemListElement')
        except:
            products = {}

        if products:
            for product in products:
                product_link = "https://" + product.get("url")
                # print(product_link)

                metadata = {}
                metadata['product_url'] = product_link
                final_url = SCRAPER_URL + product_link

                if product_link not in visited_sku_list:
                    yield scrapy.Request(
                        url=final_url, 
                        callback=self.parse_product, 
                        meta={'metadata':metadata}
                    )
                else:
                    print("----PRODUCT EXISTS----")

    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']
        product_url = metadata['product_url']
        miscellaneous = {}

        scrape_date = datetime.today().strftime('%Y-%m-%d')
        try:
            string_data = response.xpath('//script[contains(text(), \'"@type": "Product"\')]/text()')[0].extract()
            product_detail = json.loads(string_data)
        except:
            product_detail = {}

        if product_detail:
            try:
                master_sku_id = product_detail.get('sku')
                if master_sku_id:
                    miscellaneous['master_sku_id'] = master_sku_id
            except:
                master_sku_id = None

            try:
                brand = product_detail.get('brand').get('name')
            except Exception as e:
                brand = None

            try:
                product_description = product_detail.get('description')
            except Exception as e:
                product_description = None

            try:
                image_url = 'https:' + product_detail.get('image')
            except Exception as e:
                image_url = None

            try:
                more_info_list = response.css('span.metafield-multi_line_text_field::text').getall()
            except Exception as e:
                more_info_list = []

            more_info_string = ' | '.join(more_info_list)

            product_variations = product_detail.get('offers')
            if isinstance(product_variations, dict):
                product_variations = [product_variations]

            if product_variations:
                for product_variation in product_variations:
                    try:
                        oos_string = product_variation.get('availability')
                        if 'instock' in oos_string.lower():
                            oos = 0
                        else:
                            oos = 1
                    except:
                        oos = None

                    try:
                        sku_id = product_variation.get('sku')
                    except:
                        sku_id = None

                    try:
                        product_name = product_variation.get('name')
                    except Exception as e:
                        product_name = None

                    try:
                        price = product_variation.get('priceSpecification').get('price')
                    except:
                        price = None

                    if self.sub_category in ['makeup']:
                        try:
                            size = product_name.split('-')[-1].strip()
                        except:
                            size = get_size_from_title(product_name)
                    else:
                        size = get_size_from_title(product_name)

                    miscellaneous_string = json.dumps(miscellaneous)

                    item['website_id'] = WEBSITE_ID
                    item['scrape_date'] = scrape_date
                    item['category'] = self.category
                    item['sub_category'] = self.sub_category
                    item['brand'] = brand
                    item['sku_id'] = sku_id
                    item['product_name'] = product_name
                    item['product_url'] = product_url
                    item['image_url'] = image_url
                    item['product_description'] = product_description
                    item['info_table'] = more_info_string
                    item['out_of_stock'] = oos
                    item['price'] = price
                    item['mrp'] = None
                    item['high_street_price'] = None
                    item['discount'] = None
                    item['size'] = size
                    item['qty_left'] = None
                    item['usd_price'] = None
                    item['usd_mrp'] = None
                    item['miscellaneous'] = miscellaneous_string

                    yield item