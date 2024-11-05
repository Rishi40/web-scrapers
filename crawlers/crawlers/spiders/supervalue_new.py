from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import time
from worldduty.items import CrawlerItem
from worldduty.common_functions import SCRAPER_URL, visited_skus, BENCHMARK_DATE, get_size_from_title, write_to_log
from datetime import datetime
from scrapy import signals
import unidecode
import requests

WEBSITE_ID = 39
PRODUCT_LIST_LIMIT = 30

sub_category_endpoint_dict = {
    'boxed-chocolates': 'chocolate-%26-sweets/boxed-chocolates-%26-gifts-id-O301300',
    'chocolate-bars': 'chocolate-%26-sweets/chocolate-bars-id-O301305',
    'chocolate-pouches': 'chocolate-%26-sweets/chocolate-pouches-%26-bags-id-O301320',
    'gum': 'chocolate-%26-sweets/gum-id-O301325',
    'mints': 'chocolate-%26-sweets/mints-id-O301327',
    'sweets': 'chocolate-%26-sweets/sweets-id-O301329',
    'seasonal': 'chocolate-%26-sweets/seasonal-id-O301332',
    'cigarettes': 'cigarettes-%26-cigars/cigarettes-id-O302560',
    'cigars': 'cigarettes-%26-cigars/cigars-id-O302565',
    'tobacco': 'cigarettes-%26-cigars/tobacco-id-O302570',
    'smoking-accessories': 'cigarettes-%26-cigars/smoking-accessories-id-O302575',
    'e-cigarettes': 'cigarettes-%26-cigars/ecigarettes-id-O302580'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_supervalu_new"
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
        sub_category_endpoint = sub_category_endpoint_dict[sub_category]
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = f'https://shop.supervalu.ie/sm/delivery/rsid/5550/categories/{sub_category_endpoint}?page=1'
        final_url = SCRAPER_URL + url
        
        yield scrapy.Request(
            url=final_url, 
            callback=self.parse_catalogue_pages,
            meta = {
                'visited_sku_list': visited_sku_list,
                'sub_category': sub_category,
                'sub_category_endpoint': sub_category_endpoint
            },
            dont_filter=True
        )

        # For Testing = page wise testing
        # url = 'https://shop.supervalu.ie/sm/delivery/rsid/5550/categories/chocolate-%26-sweets/chocolate-bars-id-O301305?page=2&skip=30'
        # final_url = SCRAPER_URL + url

        # yield scrapy.Request(
        #     url=final_url, 
        #     callback=self.parse_catalogue_links,
        #     meta = {
        #         'visited_sku_list': visited_sku_list,
        #         'sub_category': sub_category,
        #         'sub_category_endpoint': sub_category_endpoint
        #     },
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
        sub_category = response.meta['sub_category']
        sub_category_endpoint = response.meta['sub_category_endpoint']

        no_of_pages_str = response.css('li[data-testid="pageOption-list-testId"] button::text').getall()[-1]
        no_of_pages = int(no_of_pages_str)
        offset = 0
        print("NO OF PAGES ====", no_of_pages)

        # no_of_pages = 1 ## For Testing
        for page_index in range(1,no_of_pages+1):
            url = f'https://shop.supervalu.ie/sm/delivery/rsid/5550/categories/{sub_category_endpoint}?page={page_index}&skip={offset}'
            final_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=final_url, 
                callback=self.parse_catalogue_links,
                meta = {
                    'visited_sku_list':visited_sku_list,
                    'sub_category':sub_category,
                    'sub_category_endpoint':sub_category_endpoint
                },
                dont_filter=True
            )

            offset += PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        product_cards = response.css('div.ColListing--1fk1zey')

        if product_cards:
            for product in product_cards:
                item = CrawlerItem()
                miscellaneous = {}
                scrape_date = datetime.today().strftime('%Y-%m-%d')
                size = None
                oos = 0

                try:
                    product_link = product.css('a.ProductCardHiddenLink--v3c62m::attr(href)').get()
                except:
                    product_link = None

                try:
                    brand = product.css('div[data-testid="ProductCardAQABrand"]::text').get()
                except:
                    brand = None
                
                try:
                    image_url = product.css('img[data-testid="imageSSR-img-testId"]::attr(src)').get()
                except:
                    image_url = None

                try:
                    sku_id_string = product.css('article.ProductCardWrapper--6uxd5a::attr(data-testid)').get()
                    sku_id = sku_id_string.split('-')[-1]
                except:
                    sku_id = None

                product_name_selectors = [
                    'div.sc-eCApGN::text',
                    'div.sc-hKFyIo::text',
                    'div.exBCzh p::text'
                ]

                product_name = None
                try:
                    for product_name_selector in product_name_selectors:
                        product_name_string = product.css(product_name_selector).get()
                        if product_name_string:
                            product_name = product_name_string.strip()
                            break
                except Exception as e:
                    product_name = None

                try:
                    price = product.css('span.ProductCardPrice--xq2y7a::text').get().replace('€','').strip()
                except:
                    price = None

                try:
                    mrp = product.css('span.WasPrice--1iwg7oj::text').get().replace('€','').replace('was','').strip()
                except:
                    mrp = None

                try:
                    price_per_kg = product.css('span.ProductCardPriceInfo--1vvb8df::text').get() 
                    if price_per_kg:
                        price_per_kg_clean = unidecode.unidecode(price_per_kg)
                        miscellaneous['price_per_kg_clean'] = price_per_kg_clean
                except:
                    price_per_kg = None

                try:
                    size = get_size_from_title(product_name)
                except:
                    size = None

                miscellaneous_string = json.dumps(miscellaneous)

                item['website_id'] = WEBSITE_ID
                item['scrape_date'] = scrape_date
                item['category'] = self.category
                item['sub_category'] = self.sub_category
                item['brand'] = brand
                item['sku_id'] = sku_id
                item['product_name'] = product_name
                item['product_url'] = product_link
                item['image_url'] = image_url
                item['product_description'] = None
                item['info_table'] = None
                item['out_of_stock'] = oos
                item['price'] = price
                item['mrp'] = mrp
                item['high_street_price'] = None
                item['discount'] = None
                item['size'] = size
                item['qty_left'] = None
                item['usd_price'] = None
                item['usd_mrp'] = None
                item['miscellaneous'] = miscellaneous_string

                if product_link not in visited_sku_list:
                    yield item
                else:
                    print("-------PRODUCT EXISTS--------")
        
        else:
            print("-------- NO PRODUCTS EXIST ---------")
  
'''
product name
size
'''