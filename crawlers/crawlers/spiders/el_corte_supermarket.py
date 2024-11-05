from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import requests
import time
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, get_web_page, write_to_log
from datetime import datetime
from deep_translator import GoogleTranslator
from scrapy import signals

WEBSITE_ID = 21

category_pr_eng_translation = {
    'supermarket': 'supermercado'
}

sub_category_pr_eng_translation = {
    'sugar-honey': 'mercearia/acucar-e-mel',
    'cookies': 'mercearia/bolachas',
    'cereal-bars': 'mercearia/cereais-e-barras',
    'chocolates': 'mercearia/chocolates-bombons-achocolatados-e-cacaus',
    'jams': 'mercearia/doces-marmeladas-e-compotas',
    'festive-sweets': 'mercearia/doces-festivos',
    'beers-ciders': 'bebidas/cervejas-e-sidra',
    'spirits-liqueurs': 'bebidas/destilados-e-licores',
    'cellar': 'bebidas/garrafeira',
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
    name = "scrape_el_corte_supermarket"
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

        second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")

        translator = GoogleTranslator(source='pt', target='en')
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        web_pages_list = collect_web_pages(sub_category)
        pr_category = category_pr_eng_translation[category]
        pr_sub_category = sub_category_pr_eng_translation[sub_category]

        url = f'https://www.elcorteingles.pt/{pr_category}/{pr_sub_category}/1/'

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={ 
                'translator':translator, 
                'visited_sku_list':visited_sku_list,
                'web_pages_list': web_pages_list,
                'pr_category': pr_category,
                'pr_sub_category': pr_sub_category
            },
            dont_filter=True
        )

        # For Testing

        # url = 'https://www.elcorteingles.pt/supermercado/0105218600300323-super-bock-cerveja-lata-33-cl/'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['translator'] = translator

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

        translator = response.meta['translator']
        visited_sku_list = response.meta['visited_sku_list']
        web_pages_list = response.meta['web_pages_list']
        pr_category = response.meta['pr_category']
        pr_sub_category = response.meta['pr_sub_category']

        # no_of_pages_string = response.css('div.js-pagination::attr(data-pagination-total)').get()
        # no_of_pages = int(no_of_pages_string)
        # print("no_of_pages=========", no_of_pages)

        for web_pages in web_pages_list:
            page_start = web_pages[1]
            page_end = web_pages[2]

            for page_index in range(page_start,page_end+1):
                url = f"https://www.elcorteingles.pt/{pr_category}/{pr_sub_category}/{page_index}/"
                catalogue_links_url = SCRAPER_URL + url

                yield scrapy.Request(
                    url=catalogue_links_url, 
                    callback=self.parse_catalogue_links, 
                    meta={'page_index':page_index, 'translator':translator, 'visited_sku_list':visited_sku_list}, 
                    dont_filter=True
                )


    def parse_catalogue_links(self, response):

        product_cards = response.css('div.grid-item')
        translator = response.meta['translator']
        visited_sku_list = response.meta['visited_sku_list']

        if product_cards:
            for product in product_cards:
                product_link_end = product.css('a.js-product-link::attr(href)').get() 
                product_link = "https://www.elcorteingles.pt" + product_link_end

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category
                metadata['translator'] = translator

                final_url = SCRAPER_URL + product_link

                if product_link not in visited_sku_list:
                    yield scrapy.Request(
                        url=final_url, 
                        callback=self.parse_product, 
                        meta={'metadata':metadata}
                    )
                else:
                    print("----- PRODUCT EXISTS-----")

        else:
            print("------- NO PRODUCTS FOUND --------")

    def parse_product(self, response):

        item = FactiveItem()

        metadata = response.meta['metadata']
        product_url = metadata['product_url']
        translator = metadata['translator']

        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            string_data = response.xpath("//script[contains(.,'dataLayerContent')]/text()")[0].extract() 
            string_data_list = string_data.split(';')
            for element in string_data_list:
                if 'page' in element:
                    string_data_final = element
            
            string_data_clean = string_data_final.split('=')[-1]
            json_data = json.loads(string_data_clean)
        except:
            json_data = {}

        if json_data:
            try:
                brand = json_data.get('product').get('brand') 
            except Exception as e:
                brand = ''

            try:
                product_name = json_data.get('product').get('name')  
            except Exception as e:
                product_name = ''

            try:
                product_name_en = translator.translate(product_name)
            except:
                product_name_en = product_name

            try:
                product_description_list = response.css('div.product_detail-description-in-image p::text').getall()
                product_description = ' '.join(product_description_list)
            except:
                product_description = None

            try:
                if product_description.strip():
                    product_description_en = translator.translate(product_description)
                else:
                    product_description_en = None
            except:
                product_description_en = product_description

            size = get_size_from_title(product_name_en)

            table_data = response.css('ul.info-list li.info-item')
            more_info_list = []

            try:
                for data in table_data:
                    key = data.css('span.info-key::text').get()
                    value = data.css('::text').getall()[-1]

                    if key and value and key!=value:
                        dict_string = key + value
                        try:
                            dict_string_en = translator.translate(dict_string)
                        except:
                            dict_string_en = dict_string

                        more_info_list.append(dict_string_en)

                if more_info_list:
                    more_info_string_en = ' | '.join(more_info_list)
                else:
                    more_info_string_en = None

            except Exception as e:
                more_info_string_en = None
                
            try:
                sku_id = json_data.get('product').get('id').replace('_','')
            except Exception as e:
                sku_id = ''

            try:
                image_url = "https:" + response.css('img.js-zoom-to-modal-image::attr(data-zoom)').get()
            except:
                image_url = None

            try:
                price = json_data.get('product').get('price').get('final') 
            except:
                price = None

            try:
                oos_string = json_data.get('product').get('status') 
                if 'available' in oos_string.lower():
                    oos = 0
                else:
                    oos = 1
            except:
                oos = None

            try:
                qty_left = json_data.get('product').get('quantity')
            except:
                qty_left = None

            gtin_selectors = [
                'span[itemprop="gtin13"]::text',
                'div.reference-container span.hidden::text',
            ]

            gtin = None
            try:
                for gtin_selector in gtin_selectors:
                    gtin = response.css(gtin_selector).get()
                    if gtin:
                        miscellaneous['gtin'] = gtin
                        break
            except Exception as e:
                gtin = None

            miscellaneous_string = json.dumps(miscellaneous)

            item['website_id'] = WEBSITE_ID
            item['scrape_date'] = scrape_date
            item['category'] = metadata['category']
            item['sub_category'] = metadata['sub_category']
            item['brand'] = brand
            item['sku_id'] = sku_id
            item['product_name'] = product_name_en
            item['product_url'] = product_url
            item['image_url'] = image_url
            item['product_description'] = product_description_en
            item['info_table'] = more_info_string_en
            item['out_of_stock'] = oos
            item['price'] = price
            item['mrp'] = None
            item['high_street_price'] = None
            item['discount'] = None
            item['size'] = size
            item['qty_left'] = qty_left
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = miscellaneous_string

            yield item 
