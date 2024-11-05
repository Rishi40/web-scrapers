from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import get_size_from_title, SCRAPER_URL, visited_skus, get_web_page, BENCHMARK_DATE, write_to_log
from datetime import datetime
import time
from scrapy import signals

WEBSITE_ID = 26
PRODUCT_LIST_LIMIT = 96

def collect_web_pages(sub_category):
    web_pages_list = []

    while True:
        web_page_tuple = get_web_page(WEBSITE_ID,sub_category)
        print("PICKED ==>", web_page_tuple)
        if web_page_tuple:
            web_pages_list.append(web_page_tuple)
            time.sleep(30)
        else:
            break

    return web_pages_list

class WdcSpider(scrapy.Spider):
    name = "scrape_saq"
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.CrawlerPipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category

        url  = f'https://www.saq.com/en/products?p=1&product_list_limit={PRODUCT_LIST_LIMIT}'

        # second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        web_pages_list = collect_web_pages(sub_category)

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={
                'visited_sku_list':visited_sku_list,
                'web_pages_list': web_pages_list,
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.saq.com/en/12705631'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # yield scrapy.Request(url=final_url, callback=self.parse_product,meta={'metadata':metadata})

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
        web_pages_list = response.meta['web_pages_list']

        for web_pages in web_pages_list:
            page_start = web_pages[1]
            page_end = web_pages[2]

            for page_index in range(page_start,page_end+1):
                url = f'https://www.saq.com/en/products?p={page_index}&product_list_limit={PRODUCT_LIST_LIMIT}'
                catalogue_links_url = SCRAPER_URL + url

                yield scrapy.Request(
                    url=catalogue_links_url, 
                    callback=self.parse_catalogue_links, 
                    meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                    dont_filter=True)


    def parse_catalogue_links(self, response):
        page_index = response.meta['page_index']
        visited_sku_list = response.meta['visited_sku_list']
        product_links = response.css('a.product-item-link::attr(href)').getall()

        for product_link in product_links:
            metadata = {}
            metadata['product_url'] = product_link
            metadata['page_index'] = page_index
            metadata['category'] = self.category
            metadata['sub_category'] = self.sub_category
            final_url = SCRAPER_URL + product_link

            if product_link not in visited_sku_list:
                yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata}, dont_filter=True)
            else:
                print("----- PRODUCT EXISTS -----")
                
    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']
        miscellaneous = {}

        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            string_data = response.xpath("//script[contains(.,'description')]/text()")[0].extract()
            json_data = json.loads(string_data)
        except:
            json_data = {}

        if json_data:
            try:
                sku_id = json_data.get('sku')
            except:
                sku_id = None

            try:
                product_name = json_data.get('name')
            except Exception as e:
                product_name = ''

            try:
                product_url = json_data.get('offers').get('url')
            except Exception as e:
                product_url = ''

            try:
                image_url = json_data.get('image')
            except Exception as e:
                image_url = ''

            try:
                product_description = json_data.get('description')
            except Exception as e:
                product_description = ''

            try:
                oos_string = json_data.get('offers').get('availability').lower()
                if 'instock' in oos_string:
                    oos = 0
                else:
                    oos = 1
            except Exception as e:
                print("OOS Exception ==>",e)
                oos = None

            try:
                price = json_data.get('offers').get('price')
            except:
                price = None

        try:
            reviewCount = response.css('span[itemprop="reviewCount"]::text').get()
            miscellaneous['reviewCount'] = reviewCount
        except:
            reviewCount = None

        try:
            ratingList = response.css('div.rating-result span::text').getall()
            rating = ' '.join([rating.strip() for rating in ratingList]).strip()
            miscellaneous['rating'] = rating
        except:
            rating = None

        try:
            mrp = response.css('span[data-price-type="oldPrice"]::attr(data-price-amount)').get() 
        except:
            mrp = None

        try:
            discount = response.css('span.special-price-wording::text').get().strip()
        except:
            discount = None

        try:
            size = response.css('strong[data-th="Size"]::text').get().strip()
        except:
            size = None

        try:
            table_data = response.css('ul.list-attributs li')
            more_info_list = []
            for data in table_data:
                key = data.css('span::text').get().strip()
                value = data.css('strong::text').get().strip()

                if key and value:
                    dict_string = key + ": " + value

                more_info_list.append(dict_string)

            more_info_string = ' | '.join(more_info_list).replace('\xa0',' ')
        except:
            pass

        try:
            tasting_container = response.css('ul.tasting-container li')
            tasting_list = []
            for data in tasting_container:
                key = data.css('span::text').get().strip()
                value = data.css('strong::text').get().strip()

                if key and value:
                    dict_string = key + ": " + value

                tasting_list.append(dict_string)
            tasting_string = ' | '.join(tasting_list)
            if tasting_string:
                miscellaneous['tasting_string'] = tasting_string
        except:
            tasting_string = None

        
        miscellaneous_string = json.dumps(miscellaneous)

        item['website_id'] = WEBSITE_ID
        item['scrape_date'] = scrape_date
        item['category'] = metadata['category']
        item['sub_category'] = metadata['sub_category']
        item['brand'] = None
        item['sku_id'] = sku_id
        item['product_name'] = product_name
        item['product_url'] = product_url
        item['image_url'] = image_url
        item['product_description'] = product_description
        item['info_table'] = more_info_string
        item['out_of_stock'] = oos
        item['price'] = price
        item['mrp'] = mrp
        item['high_street_price'] = None
        item['discount'] = discount
        item['size'] = size
        item['qty_left'] = None
        item['usd_price'] = None
        item['usd_mrp'] = None
        item['miscellaneous'] = miscellaneous_string

        yield item


'''
update to 0 if it is 1
page splits should be done by code
'''