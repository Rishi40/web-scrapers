from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import requests
from worldduty.items import CrawlerItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_miscelleneous_parameter, visited_sku_ids, BENCHMARK_DATE, write_to_log
from datetime import datetime
import math
from deep_translator import GoogleTranslator
from scrapy import signals

WEBSITE_ID = 24
PRODUCT_LIST_LIMIT = 200

pr_eng_translation = {
    'fragrance': 'perfumes',
    'make-up': 'maquilhagem',
    'skin-care': 'cosmetica',
}

class WdcSpider(scrapy.Spider):
    name = "scrape_perfumes_companhia"
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
        pr_sub_category = pr_eng_translation[sub_category]
        second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        visited_master_list = visited_miscelleneous_parameter(WEBSITE_ID,BENCHMARK_DATE,sub_category,'master_sku_id')

        translator = GoogleTranslator(source='pt', target='en')

        url = f'https://www.perfumesecompanhia.pt/pt/{pr_sub_category}/'

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={'pr_sub_category':pr_sub_category, 'translator':translator, 'visited_master_list':visited_master_list},
            dont_filter=True
        )

        # For Testing

        # url = 'https://www.perfumesecompanhia.pt/pt/dolceandgabbana-k-by-dolce-gabbana-eau-de-parfum/49434.html?undefined'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['translator'] = translator
        # metadata['master_sku_id'] = '26318M'

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

        pr_sub_category = response.meta['pr_sub_category']
        translator = response.meta['translator']
        visited_master_list = response.meta['visited_master_list']

        try:
            product_count = response.css('span.search-result-count::text').getall()[-1]
            product_count = product_count.strip()
        except:
            print("Failed")
            product_count = 0

        print("====PRODUCT COUNT====",product_count)
        no_of_pages = int(product_count)//PRODUCT_LIST_LIMIT + 1
        offset = 0

        # no_of_pages = 2 ## For Testing
        for page_index in range(no_of_pages):
            url = f'https://www.perfumesecompanhia.pt/on/demandware.store/Sites-PC-Site/pt_PT/Search-UpdateGrid?cgid={pr_sub_category}&start={offset}&sz={PRODUCT_LIST_LIMIT}'
            
            catalogue_links_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=catalogue_links_url, 
                callback=self.parse_catalogue_links, 
                meta={'page_index':page_index,'translator':translator,'visited_master_list':visited_master_list}, 
                dont_filter=True)

            offset = offset + PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):

        product_cards = response.css('div.js-search-product-tile')
        translator = response.meta['translator']
        visited_master_list = response.meta['visited_master_list']

        if product_cards:
            for product in product_cards:
                master_sku_id = product.css('div.product::attr(data-pid)').get() 
                product_link_end = product.css('div.pdp-link a::attr(href)').get() 
                product_link = "https://www.perfumesecompanhia.pt" + product_link_end

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category
                metadata['translator'] = translator
                metadata['master_sku_id'] = master_sku_id

                final_url = SCRAPER_URL + product_link

                if master_sku_id.strip() not in visited_master_list:
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata}, dont_filter=True)
                else:
                    print("-------- PRODUCT ALREADY EXISTS --------")


    def parse_product(self, response):

        item = CrawlerItem()
        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        metadata = response.meta['metadata']
        product_url = metadata['product_url']
        translator = metadata['translator']
        master_sku_id = metadata['master_sku_id']
        miscellaneous['master_sku_id'] = master_sku_id


        size_object_list = response.css('button.js-product-size')
        color_object_list = response.css('button.color-attribute')

        if size_object_list:
            for size_object in size_object_list:
                size = size_object.css('button.js-product-size::attr(data-attr-value)').get()
                size_url = f'https://www.perfumesecompanhia.pt/on/demandware.store/Sites-PC-Site/pt_PT/Product-Variation?dwvar_{master_sku_id}_capacidade={size}&pid={master_sku_id}&quantity=1'

                try:
                    string_data = requests.request("GET", size_url)
                    json_data = json.loads(string_data.text)
                except Exception as e:
                    json_data = {}


                if json_data:
                    try:
                        brand = json_data.get('product').get('brand')
                    except Exception as e:
                        brand = ''

                    try:
                        sku_id = json_data.get('product').get('id')
                    except:
                        sku_id = None

                    try:
                        product_line = json_data.get('product').get('line')
                        name = json_data.get('product').get('productName')
                        product_name = product_line + ' ' + name
                    except:
                        product_name = None
                    
                    try:
                        product_name_en = translator.translate(product_name)
                    except:
                        product_name_en = product_name

                    try:
                        product_description = json_data.get('product').get('longDescription')
                    except:
                        product_description = None

                    try:
                        product_description_en = translator.translate(product_description)
                    except:
                        product_description_en = product_description

                    try:
                        product_url = json_data.get('product').get('canonicalUrl')
                    except:
                        product_url = None

                    try:
                        discount = json_data.get('product').get('discountPercentage')
                    except:
                        discount = None

                    try:
                        image_url = json_data.get('product').get('images').get('hi-res')[0].get('url')
                    except:
                        image_url = None

                    try:
                        price = json_data.get('product').get('price').get('sales').get('value')
                    except:
                        price = None

                    try:
                        mrp = json_data.get('product').get('price').get('list').get('value')
                    except:
                        mrp = None

                    try:
                        oos_string = json_data.get('product').get('available')
                        if oos_string:
                            oos = 0
                        else:
                            oos = 1
                    except:
                        oos = None

                    miscellaneous_string = json.dumps(miscellaneous)

                    item['website_id'] = WEBSITE_ID
                    item['scrape_date'] = scrape_date
                    item['category'] = metadata['category']
                    item['sub_category'] = metadata['sub_category']
                    item['brand'] = brand
                    item['sku_id'] = sku_id
                    item['product_name'] = product_name
                    item['product_url'] = product_url
                    item['image_url'] = image_url
                    item['product_description'] = product_description_en
                    item['info_table'] = None
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

        elif color_object_list:
            for color_object in color_object_list:
                color_code = color_object.css('button.color-attribute::attr(aria-describedby)').get()
                color_url = f'https://www.perfumesecompanhia.pt/on/demandware.store/Sites-PC-Site/pt_PT/Product-Variation?dwvar_{master_sku_id}_color={color_code}&pid={master_sku_id}&quantity=1&isQuickView=false&isPDP=true'

                try:
                    string_data = requests.request("GET", color_url)
                    json_data = json.loads(string_data.text)
                except Exception as e:
                    json_data = {}


                if json_data:
                    size = None
                    miscellaneous['color_code'] = color_code

                    try:
                        brand = json_data.get('product').get('brand')
                    except Exception as e:
                        brand = ''

                    try:
                        sku_id = json_data.get('product').get('id')
                    except:
                        sku_id = None

                    try:
                        product_line = json_data.get('product').get('line')
                        name = json_data.get('product').get('productName')
                        product_name = product_line + ' ' + name

                        if product_name:
                            try:
                                product_name_en = translator.translate(product_name)
                            except:
                                product_name_en = product_name

                    except Exception as e:
                        product_name_en = None

                    try:
                        product_description = json_data.get('product').get('longDescription')
                    except:
                        product_description = None

                    try:
                        product_description_en = translator.translate(product_description)
                    except:
                        product_description_en = product_description

                    try:
                        color = json_data.get('product').get('variationAttributes')[0].get('displayValue')
                        miscellaneous['color'] = color
                    except:
                        color = None

                    try:
                        product_url = json_data.get('product').get('canonicalUrl')
                    except:
                        product_url = None

                    try:
                        discount = json_data.get('product').get('discountPercentage')
                    except:
                        discount = None

                    try:
                        image_url = json_data.get('product').get('images').get('hi-res')[0].get('url')
                    except:
                        image_url = None

                    try:
                        price = json_data.get('product').get('price').get('sales').get('value')
                    except:
                        price = None

                    try:
                        mrp = json_data.get('product').get('price').get('list').get('value')
                    except:
                        mrp = None

                    try:
                        oos_string = json_data.get('product').get('available')
                        if oos_string:
                            oos = 0
                        else:
                            oos = 1
                    except:
                        oos = None

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
                    item['info_table'] = None
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

        else:
            try:
                sku_id = response.css('div.js-product::attr(data-pid)').get()
            except:
                sku_id = None

            try:
                price = response.css('div.sales span::attr(content)').get()
            except:
                price = None

            try:
                image_url = response.css('img.pc-picture-img::attr(src)').get()
            except:
                image_url = None

            try:
                mrp = response.css('div.js-price-cart-container div::attr(content)').get()
            except:
                mrp = None
            
            try:
                brand = response.css('h3.product-brand::text').get()
            except:
                brand = None
            
            try:
                product_name = response.css('h5.product-line::text').get() + ' ' + response.css('h1.product-name::text').get()
            except:
                product_name = None

            try:
                product_name_en = translator.translate(product_name)
            except:
                product_name_en = product_name

            try:
                discount = response.css('span.pc-discount-badge-text::text').get().replace('-','').replace('%','')
            except:
                discount = None
            
            try:
                oos_string = response.css('button.js-notify-me::text').get()
                if oos_string:
                    oos = 1
                else:
                    oos = 0
            except:
                oos = None

            try:
                product_description = response.css('div#collapsible-details-description::text').get().strip()
                if product_description:
                    try:
                        product_description_en = translator.translate(product_description)
                    except:
                        product_description_en = product_description
            except:
                product_description = None

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
            item['info_table'] = None
            item['out_of_stock'] = oos
            item['price'] = price
            item['mrp'] = mrp
            item['high_street_price'] = None
            item['discount'] = discount
            item['size'] = None
            item['qty_left'] = None
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = miscellaneous_string

            yield item 
