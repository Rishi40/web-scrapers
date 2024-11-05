from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 29
PRODUCT_LIST_LIMIT = 80

sub_category_id_map = {
    'irish-whiskey': 1,
    'scottish-whiskey': 2,
    'world-whiskey': 3,
    'irish-spirits': 4,
    'other-spirits': 5,
    'wine': 6
}

headers = {
    'authority': 'www.celticwhiskeyshop.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.celticwhiskeyshop.com',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

def get_product_description(response):
    product_description_raw_list = []

    main_description_list = [
        'div.descriptiondesktop p::text',
        'div.descriptiondesktop p span::text',
    ]

    for md in main_description_list:
        desc_list = response.css(md).getall()
        desc_list_clean = [desc.strip() for desc in desc_list if desc.strip()] 
        product_description_raw_list.append(desc_list_clean)

    product_description_list = [item for pd in product_description_raw_list for item in pd]

    product_description = '\n'.join(product_description_list)
    return product_description


class WdcSpider(scrapy.Spider):
    name = "scrape_celtic_whiskey"
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
        sub_category_id = sub_category_id_map[sub_category]

        url = 'https://www.celticwhiskeyshop.com/index.php?route=module/journal2_super_filter/products&module_id=54'

        second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        main_payload = f'filters=/limit=80/page=1&route=product/category&path={sub_category_id}&manufacturer_id=&search=&tag='

        yield scrapy.Request(
            method="POST",
            url=url, 
            body=main_payload,
            headers=headers,
            callback=self.parse_catalogue_pages,
            meta = {'visited_sku_list':visited_sku_list,'sub_category_id':sub_category_id},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.celticwhiskeyshop.com/21C%20Limited%20Edition%20Batch%202%20(2019%20Whiskey%20Live%20Exclusive)'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
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
        sub_category_id = response.meta['sub_category_id']

        try:
            last_page_link = response.css('ul.pagination li a::attr(href)').getall()[-1]
            no_of_pages_string = last_page_link.split('?')[-1].split('=')[-1]
            no_of_pages = int(no_of_pages_string)
            print("NO OF PAGES ====", no_of_pages)

            # no_of_pages = 2 ## For Testing
            for page_index in range(1,no_of_pages+1):
                url = 'https://www.celticwhiskeyshop.com/index.php?route=module/journal2_super_filter/products&module_id=54'
                page_payload = f'filters=/limit=80/page={page_index}&route=product/category&path={sub_category_id}&manufacturer_id=&search=&tag='

                yield scrapy.Request(
                    method="POST",
                    url=url, 
                    body=page_payload,
                    headers=headers,
                    callback=self.parse_catalogue_links,
                    meta = {'visited_sku_list':visited_sku_list,'sub_category_id':sub_category_id},
                    dont_filter=True
                )

        except Exception as e:
            print("=====ERROR IN FETCHING PAGE NUMBER==========")

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        product_cards = response.css('div.product-list-item')

        for product in product_cards:
            product_link = product.css('h4.name a::attr(href)').get()

            metadata = {}
            metadata['product_url'] = product_link
            final_url = SCRAPER_URL + product_link

            if product_link not in visited_sku_list:
                yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata})
            else:
                print("----PRODUCT EXISTS----")

    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']

        scrape_date = datetime.today().strftime('%Y-%m-%d')
        product_info = response.css('div.product-info')

        try:
            sku_id = response.css('input[name="product_id"]::attr(value)').get() 
        except:
            sku_id = None

        try:
            # product_name = response.css('h1[itemprop="name"]::text').get() #old
            product_name = response.css('h1.heading-title::text').get().strip()
        except Exception as e:
            product_name = None

        try:
            product_url = response.css('input[name="redirect"]::attr(value)').get() 
        except Exception as e:
            product_url = None

        try:
            image_url = response.css('img#image::attr(src)').get() 
        except Exception as e:
            image_url = None

        try:
            product_description = get_product_description(response)
        except Exception as e:
            product_description = None

        try:
            oos_string = product_info.css('span.button-text').get()
            if oos_string:
                oos = 1
            else:
                oos = 0
        except:
            oos = None

        try:
            # price = response.css('li[itemprop="price"]::text').get().replace('€','') #old
            price = product_info.css('li.leftprice::text').get().replace('€','').strip()
        except:
            price = None

        try:
            size = response.css('div.left1 div::text').get() 
        except:
            size = None

        table_data = response.css('div#tab-specification table tbody tr') 
        more_info_list = []

        try:
            for data in table_data:
                key = data.css('td::text').getall()[0]
                value = data.css('td::text').getall()[-1]
                if key != value:
                    dict_string = key + ": " + value
                    more_info_list.append(dict_string)
        except:
            more_info_list = []

        more_info_string = ' | '.join(more_info_list)

        item['website_id'] = WEBSITE_ID
        item['scrape_date'] = scrape_date
        item['category'] = self.category
        item['sub_category'] = self.sub_category
        item['brand'] = None
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
        item['miscellaneous'] = None

        yield item
