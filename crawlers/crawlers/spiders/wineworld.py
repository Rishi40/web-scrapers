from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import clean_name, get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 37

def get_product_description(response):
    product_description_object = response.css('div.product.attribute.description')
    if not product_description_object:
        product_description_object = response.css('div.product.attibute.description')

    product_description_raw_list = []

    main_description_list = [
        '::text',
        'div::text',
        'p font::text'
    ]

    for md in main_description_list:
        desc_list = product_description_object.css(md).getall()
        desc_list_clean = [desc.strip() for desc in desc_list if desc.strip()] 
        product_description_raw_list.append(desc_list_clean)

    product_description_list = [item for pd in product_description_raw_list for item in pd]

    product_description = '\n'.join(product_description_list)
    return product_description

class WdcSpider(scrapy.Spider):
    name = "scrape_wine_world"
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

        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = f'https://www.wineworldinc.com/{sub_category}.html?product_list_limit=all'
        catalogue_url = SCRAPER_URL + url

        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_links,
            meta={
                'visited_sku_list': visited_sku_list
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.wineworldinc.com/958-santero-bellini-peach-75cl.html'
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

    def parse_catalogue_links(self, response):

        visited_sku_list = response.meta['visited_sku_list']

        product_cards = response.css('ol.product-items li.product-item')

        if product_cards:
            for product in product_cards:
                product_link = product.css('a.product-item-photo::attr(href)').get()

                metadata = {}
                metadata['product_url'] = product_link

                if product_link and product_link not in visited_sku_list:
                    final_url = SCRAPER_URL + product_link
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata})
                else:
                    # PRODUCT LINK NULL IS ALSO A CASE
                    print("----PRODUCT EXISTS----")
        
        else:
            print("-------- NO PRODUCTS EXIST ---------")

    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']
        product_link = metadata['product_url']
        size = None

        scrape_date = datetime.today().strftime('%Y-%m-%d')
        product_box = response.css('div.product-info-main')

        try:
            sku_id = response.css('div[itemprop="sku"]::text').get() 
        except:
            sku_id = None

        try:
            product_name = response.css('h1.product-name::text').get() 
        except Exception as e:
            product_name = None

        try:
            image_url = response.css('img.img-fluid::attr(src)').get()
        except:
            image_url = None

        try:
            product_description = get_product_description(response)
        except Exception as e:
            product_description = ''

        try:
            oos_string = response.css('div.stock.available span::text').get() 

            if 'in stock' in oos_string.lower():
                oos = 0
            else:
                oos = 1

        except Exception as e:
            print("OOS Exception ==>",e)
            oos = None

        try:
            price = product_box.css('span[data-price-type="finalPrice"]::attr(data-price-amount)').get()
        except:
            price = None

        try:
            mrp = product_box.css('span[data-price-type="oldPrice"]::attr(data-price-amount)').get()
        except:
            mrp = None

        try:
            table_data = response.css('table#product-attribute-specs-table tbody tr')
            more_info_list = []

            for data in table_data:
                key = data.css('th::text').get().strip()
                value = data.css('td::text').get().strip()
                if key and value:
                    dict_string = key + ": " + value
                    if 'size' in key.lower():
                        size = value

                    more_info_list.append(dict_string)

            if more_info_list:
                more_info_string = ' | '.join(more_info_list)
            else:
                more_info_string = None
        
        except:
            pass

        if not size:
            size = get_size_from_title(product_name)

        item['website_id'] = WEBSITE_ID
        item['scrape_date'] = scrape_date
        item['category'] = self.category
        item['sub_category'] = self.sub_category
        item['brand'] = None
        item['sku_id'] = sku_id
        item['product_name'] = product_name
        item['product_url'] = product_link
        item['image_url'] = image_url
        item['product_description'] = product_description
        item['info_table'] = more_info_string
        item['out_of_stock'] = 1 if oos else 0
        item['price'] = price
        item['mrp'] = mrp
        item['high_street_price'] = None
        item['discount'] = None
        item['size'] = size
        item['qty_left'] = None
        item['usd_price'] = None
        item['usd_mrp'] = None
        item['miscellaneous'] = None

        yield item