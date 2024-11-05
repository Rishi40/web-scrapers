from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import SCRAPER_URL, visited_sku_ids, BENCHMARK_DATE, write_to_log
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 53

sub_category_endpoint_map = {
    'men-sunglasses': 'men-shop-all',
    'women-sunglasses': 'women-shop-all'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_sunglasshut"
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.FactivePipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category
        sub_category_endpoint = sub_category_endpoint_map[sub_category]

        url = f'https://ae.sunglasshut.com/collections/{sub_category_endpoint}'
        final_url = SCRAPER_URL + url
        visited_sku_list = visited_sku_ids(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        yield scrapy.Request(
            url=final_url, 
            callback=self.parse_catalogue_pages,
            meta = {
                'visited_sku_list':visited_sku_list,
                'sub_category_endpoint':sub_category_endpoint
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://ae.sunglasshut.com/collections/women-shop-all/products/0pr-17ws-49-1425s0'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['product_url'] = url
        # yield scrapy.Request(
        #     url=final_url, 
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
        sub_category_endpoint = response.meta['sub_category_endpoint']

        try:
            no_of_pages_string = response.css('ul.pagination--numbers li a::text').getall()[-1].strip()
            no_of_pages = int(no_of_pages_string)
        except:
            print("===error in fetching page number====")

        # no_of_pages = 3 ## For Testing
        print("no_of_pages--->",no_of_pages)

        for page_index in range(1,no_of_pages+1):
            url = f'https://ae.sunglasshut.com/collections/{sub_category_endpoint}?page={page_index}#collection-root'

            yield scrapy.Request(
                url=SCRAPER_URL + url, 
                callback=self.parse_catalogue_links,
                meta = {
                    'visited_sku_list':visited_sku_list,
                    'sub_category_endpoint':sub_category_endpoint
                },
                dont_filter=True
            )

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        product_cards = response.css('div.collection--body--grid div.product--root')

        for product in product_cards:
            sku_id = product.css('select.product-form--variant-select option::attr(data-sku)').get()
            product_link_endpoint = product.css('a::attr(href)').get()
            product_link = 'https://ae.sunglasshut.com' + product_link_endpoint

            metadata = {}
            metadata['product_url'] = product_link
            final_url = SCRAPER_URL + product_link
            # print("-------->",product_link)
            if sku_id not in visited_sku_list:
                yield scrapy.Request(
                    url=final_url, 
                    callback=self.parse_product, 
                    meta={'metadata':metadata}
                )
            else:
                print("----PRODUCT EXISTS----")

    def parse_product(self, response):

        item = FactiveItem()
        metadata = response.meta['metadata']
        scrape_date = datetime.today().strftime('%Y-%m-%d')
        product_container = response.css('div.product-page--root')
        miscellaneous = {}

        try:
            sku_id = product_container.css('select.product-form--variant-select option::attr(data-sku)').get()
        except:
            sku_id = None

        try:
            brand = product_container.css('div.product-page--vendor a::attr(content)').get()
        except:
            brand = None

        try:
            product_name = product_container.css('h2.product-page--title::text').get().strip()
        except:
            product_name = None

        try:
            product_url = metadata['product_url']
        except:
            product_url = None

        try:
            image_url_string = 'https:' + product_container.css('div.image--container img::attr(data-src)').get()
            image_url = image_url_string.replace('{width}','1000')
        except:
            image_url = None

        try:
            oos_string = product_container.css('select.product-form--variant-select option::attr(data-available)').get()
            if oos_string.lower() == 'true':
                oos = 0
            else:
                oos = 1
        except:
            oos = None

        try:
            price = product_container.css('div.product-form--price::text').get().strip()
        except:
            price = None

        try:
            mrp = product_container.css('div.product-form--compare-price::text').get().strip()
        except:
            mrp = None

        try:
            qty_left = product_container.css('select.product-form--variant-select option::attr(data-inventory-quantity)').get()
        except:
            qty_left = None

        option_dict = {}
        try:
            option_selector = product_container.css('div.disclosure--root')
            for option in option_selector:
                key = option.css('label.disclosure--label::text').get().strip().lower()
                value = option.css('span.disclosure--current-option::text').get().strip()
                option_dict[key] = value
        except:
            option_dict = {}

        try:
            size = option_dict.get('size') 
        except:
            size = None

        try:
            color = option_dict.get('color') 
            if color:
                miscellaneous['color'] = color
        except:
            color = None

        table_data = product_container.css('div.product-page--description p')
        more_info_list = []

        try:
            for data in table_data:
                info = data.css('::text').get()
                if info:
                    more_info_list.append(info)
        except:
            more_info_list = []

        more_info_string = ' | '.join(more_info_list)

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
        item['product_description'] = None
        item['info_table'] = more_info_string
        item['out_of_stock'] = oos
        item['price'] = price
        item['mrp'] = mrp
        item['high_street_price'] = None
        item['discount'] = None
        item['size'] = size
        item['qty_left'] = qty_left
        item['usd_price'] = None
        item['usd_mrp'] = None
        item['miscellaneous'] = miscellaneous_string

        yield item