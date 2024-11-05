from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import time
from worldduty.items import CrawlerItem
from worldduty.common_functions import SCRAPER_URL, visited_skus, BENCHMARK_DATE, get_size_from_title, get_web_page, write_to_log
from datetime import datetime
from deep_translator import GoogleTranslator
import unidecode
from scrapy import signals

WEBSITE_ID = 36

sub_category_pr_eng_translation = {
    'wine': 'vinho',
    'whisky': 'whisky',
    'harbor': 'porto',
    'distillates': 'destilados',
    'pub': 'bar',
    'generous': 'generosos'
}

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
    name = "scrape_garrafeiranacional"
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
        pr_sub_category = sub_category_pr_eng_translation[sub_category]
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        translator = GoogleTranslator(source='pt', target='en')
        web_pages_list = collect_web_pages(sub_category)

        url = f'https://www.garrafeiranacional.com/{pr_sub_category}.html?p=1'
        
        yield scrapy.Request(
            url=url, 
            callback=self.parse_catalogue_pages,
            meta = {
                'visited_sku_list':visited_sku_list,
                'sub_category':sub_category,
                'pr_sub_category': pr_sub_category,
                'web_pages_list': web_pages_list,
                'translator':translator
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.garrafeiranacional.com/destilados/aguardente-j-a-s-l-202-preparada-velha-75cl.html'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['translator'] = translator

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
        web_pages_list = response.meta['web_pages_list']
        sub_category = response.meta['sub_category']
        pr_sub_category = response.meta['pr_sub_category']
        translator = response.meta['translator']

        # page_object = response.css('div.pager-desktop')
        # no_of_pages_string = page_object.css('li.item a.page.last span::text').getall()[-1]
        # no_of_pages = int(no_of_pages_string)

        # no_of_pages = 3 ## For Testing
        for web_pages in web_pages_list:
            page_start = web_pages[1]
            page_end = web_pages[2]

            for page_index in range(page_start,page_end+1):

                url = f'https://www.garrafeiranacional.com/{pr_sub_category}.html?p={page_index}'
                # print("----------------->",url)
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse_catalogue_links,
                    meta = {
                        'visited_sku_list':visited_sku_list,
                        'sub_category':sub_category,
                        'translator':translator
                    },
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        translator = response.meta['translator']

        product_cards = response.css('div.container div.slide')

        if product_cards:
            for product in product_cards:
                product_link = product.css('a.product-item-link::attr(href)').get()

                metadata = {}
                metadata['product_url'] = product_link
                metadata['translator'] = translator

                final_url = SCRAPER_URL + product_link

                # print(product_link)
                if product_link not in visited_sku_list:
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata})
                else:
                    print("-------PRODUCT EXISTS--------")
        
        else:
            print("-------- NO PRODUCTS EXIST ---------")

    def parse_product(self, response):

        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        variation_list = response.css('ul.bottle_litre a::attr(href)').getall() 

        if variation_list:
            for variation in variation_list:
                metadata['product_url'] = variation
                final_url = SCRAPER_URL + variation

                yield scrapy.Request(
                    url=final_url, 
                    callback=self.parse_product_variations, 
                    meta={'metadata':metadata},
                    dont_filter=True
                )

        else:
            final_url = SCRAPER_URL + product_url
            yield scrapy.Request(
                url=final_url, 
                callback=self.parse_product_variations, 
                meta={'metadata':metadata},
                dont_filter=True
            )

    def parse_product_variations(self, response):

        item = CrawlerItem()

        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        metadata = response.meta['metadata']
        product_url = metadata['product_url']
        translator = metadata['translator']
        size = None

        product_box = response.css('div.prod_detail_part1')
        
        try:
            sku_id = response.css('form#product_addtocart_form::attr(data-product-sku)').get() 
        except:
            sku_id = None

        try:
            product_name = response.css('span[itemprop="name"]::text').get() 
        except Exception as e:
            product_name = None

        try:
            product_name_en = translator.translate(product_name)
        except:
            product_name_en = product_name

        try:
            image_url = response.css('div.gallery_main_image img::attr(src)').get()
        except Exception as e:
            image_url = None

        try:
            price = product_box.css('span[data-price-type="finalPrice"]::attr(data-price-amount)').get()
        except:
            price = None

        try:
            mrp = product_box.css('span[data-price-type="oldPrice"]::attr(data-price-amount)').get()
        except:
            mrp = None

        try:
            discount = product_box.css('span.price_discount_amount::text').get().replace('-','').replace('%','').strip()
        except:
            discount = None

        try:
            oos_string = response.css('button#product-addtocart-button span::text').get() 
            if oos_string.lower() == 'adicionar':
                oos = 0
            else:
                oos = 1
        except:
            oos = None

        try:
            table_data = response.css('div.characteristics div.char_name')
            more_info_list = []

            try:
                for data in table_data:
                    key = data.css('h6::text').get().strip()
                    value = data.css('p a::text').get().strip()
                    if key and value:
                        dict_string = key + ": " + value
                        if 'capacidade' in key.lower():
                            size = value
                        try:
                            dict_string_en = translator.translate(dict_string)
                        except:
                            dict_string_en = dict_string

                        more_info_list.append(dict_string_en)

                if more_info_list:
                    more_info_string_en = ' | '.join(more_info_list)
                else:
                    more_info_string_en = None

            except:
                more_info_list = []
        
        except:
            pass

        if not size:
            size = get_size_from_title(product_name_en)

        try:
            price_with_currency = product_box.css('span[data-price-type="finalPrice"] span.price::text').get().strip()
            if price_with_currency:
                price_with_currency_clean = unidecode.unidecode(price_with_currency)
                miscellaneous['price_with_currency'] = price_with_currency_clean
        except:
            price_with_currency = None

        miscellaneous_string = json.dumps(miscellaneous)

        item['website_id'] = WEBSITE_ID
        item['scrape_date'] = scrape_date
        item['category'] = self.category
        item['sub_category'] = self.sub_category
        item['brand'] = None
        item['sku_id'] = sku_id
        item['product_name'] = product_name_en
        item['product_url'] = product_url
        item['image_url'] = image_url
        item['product_description'] = None
        item['info_table'] = more_info_string_en
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
product description is general, not product specific but brand specific
do page numbers but search what happens when exceeded page number is given
'''