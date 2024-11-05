import scrapy
import json
from worldduty.items import CrawlerItem
from worldduty.common_functions import visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 49
PRODUCT_LIMIT = 96

HEADERS = {
    'authority': 'www.extime.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'if-none-match': '"16xbq7n5i9l4k9f"',
    'referer': 'https://www.extime.com/',
    'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'x-revalidate': 'CDG_1'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_extime_dubai"
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

        url = 'https://www.extime.com/api/shopping/71480?limit=96'
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        yield scrapy.Request(
            url=url, 
            callback=self.parse_catalogue_pages,
            headers=HEADERS,
            meta = {'visited_sku_list':visited_sku_list,'sub_category':sub_category},
            dont_filter=True
        )

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
            string_data = response.body
            json_data = json.loads(string_data)
            product_count = json_data.get('count')
            end_count = product_count + PRODUCT_LIMIT
            url = f'https://www.extime.com/api/shopping/71480?limit={end_count}'

            yield scrapy.Request(
                url=url, 
                callback=self.parse_catalogue_links,
                meta = {'visited_sku_list':visited_sku_list},
                dont_filter=True
            )

        except Exception as e:
            print("=====ERROR IN FETCHING INITIAL PAGE DATA==========")

    def parse_catalogue_links(self, response):

        product_cards = response.css('div.featured-box')
        visited_sku_list = response.meta['visited_sku_list']

        try:
            string_data = response.body
            json_data = json.loads(string_data) 
        except:
            json_data = {}

        if json_data:
            product_cards = json_data.get('items')
            for product in product_cards:
                item = CrawlerItem()
                miscellaneous = {}
                scrape_date = datetime.today().strftime('%Y-%m-%d')

                try:
                    brand = product.get('brand_name')
                except:
                    brand = None

                try:
                    sku_id = product.get('sku')
                except:
                    sku_id = None

                try:
                    gtin = product.get('gtin')
                    if gtin:
                        miscellaneous['gtin'] = gtin
                except:
                    gtin = None

                try:
                    product_id = product.get('id')
                    if product_id:
                        miscellaneous['product_id'] = product_id
                except:
                    product_id = None

                try:
                    product_name = product.get('product_name')
                except:
                    product_name = None

                # try:
                #     price = product.get('price')
                # except:
                #     price = None

                # try:
                #     mrp = product.get('price_crossed')
                # except:
                #     mrp = None

                try:
                    discount = product.get('duty_free').get('catalog_discount_name')[0]
                except:
                    discount = None

                # try:
                #     duty_free_price = product.get('duty_free').get('price')
                #     if duty_free_price:
                #         miscellaneous['duty_free_price'] = duty_free_price
                # except:
                #     duty_free_price = None

                # try:
                #     duty_free_mrp = product.get('duty_free').get('price_crossed')
                #     if duty_free_mrp:
                #         miscellaneous['duty_free_mrp'] = duty_free_mrp
                # except:
                #     duty_free_mrp = None

                # try:
                #     duty_paid_price = product.get('duty_paid').get('price')
                #     if duty_paid_price:
                #         miscellaneous['duty_paid_price'] = duty_paid_price
                # except:
                #     duty_paid_price = None

                # try:
                #     duty_paid_mrp = product.get('duty_paid').get('price_crossed')
                #     if duty_paid_mrp:
                #         miscellaneous['duty_paid_mrp'] = duty_paid_mrp
                # except:
                #     duty_paid_mrp = None

                try:
                    price = product.get('duty_free').get('price')
                except:
                    price = None

                try:
                    mrp = product.get('duty_paid').get('price')
                except:
                    mrp = None
                    
                try:
                    slug = product.get('slug')
                    product_url = 'https://www.extime.com/en/paris/product/' + slug
                except:
                    product_url = None

                try:
                    image_url = product.get('default_image')
                except:
                    image_url = None

                try:
                    capacity = product.get('capacity')
                    capacity_unit = product.get('capacity_unit')
                    size = str(capacity) + ' ' + capacity_unit
                except:
                    size = None

                try:
                    product_description = product.get('product_information')
                except:
                    product_description = None

                try:
                    qty_left = product.get('stock')
                except:
                    qty_left = None

                try: 
                    if str(qty_left) == '0':
                        oos = 1
                    else:
                        oos = 0
                except:
                    oos = None

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
                item['info_table'] = None
                item['out_of_stock'] = oos
                item['price'] = price
                item['mrp'] = mrp
                item['high_street_price'] = None
                item['discount'] = discount
                item['size'] = size
                item['qty_left'] = qty_left
                item['usd_price'] = None
                item['usd_mrp'] = None
                item['miscellaneous'] = miscellaneous_string

                yield item