from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import requests
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime, timedelta
import math
from scrapy import signals

WEBSITE_ID = 17
PRODUCT_LIST_LIMIT = 48

def get_product_description(response):
    product_description_raw_list = []

    main_description_list = [
        'div[itemprop="description"] span a::text',
        'div[itemprop="description"]::text',
        'div[itemprop="description"] p::text',
        'div[itemprop="description"] ul li::text',
    ]

    for md in main_description_list:
        desc_list = response.css(md).getall()
        product_description_raw_list.append(desc_list)

    product_description_list = [item for pd in product_description_raw_list for item in pd]
    product_description = ' '.join(product_description_list)

    return product_description

class WdcSpider(scrapy.Spider):
    name = "scrape_brown_thomas"
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
        # second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        previous_week_date = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,previous_week_date,sub_category)

        url = f'https://www.brownthomas.com/beauty/{sub_category}/'

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={'visited_sku_list':visited_sku_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.brownthomas.com/beauty/fragrance/happy-in-paradise-limited-edition-eau-de-parfum-spray/171403437.html?cgid=beauty-fragrance'
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

        try:
            product_count_string = response.css('span.pag-total-items-show::text').get()
            product_count = product_count_string.split()[0].replace(',','')
        except:
            print("Failed")
            product_count = 0

        print("====PRODUCT COUNT====",product_count)
        no_of_pages = int(product_count)//PRODUCT_LIST_LIMIT + 1
        offset = 0

        # no_of_pages = 1 ## For Testing
        for page_index in range(no_of_pages):
            url = f'https://www.brownthomas.com/beauty/{self.sub_category}/?format=page-element&start={offset}&sz={PRODUCT_LIST_LIMIT}&productsearch=true'
            catalogue_links_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=catalogue_links_url, 
                callback=self.parse_catalogue_links, 
                meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                dont_filter=True)

            offset = offset + PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):

        visited_sku_list = response.meta['visited_sku_list']
        product_cards = response.css('li.js-product-grid-tile')

        if product_cards:
            for product in product_cards:
                product_link = product.css('a.thumb-link::attr(href)').get() 

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category

                final_url = SCRAPER_URL + product_link
                if product_link not in visited_sku_list:
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata}, dont_filter=True)
                else:
                    print("----PRODUCT EXISTS-----")

    def parse_product(self, response):

        item = FactiveItem()
        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        brand_selectors = [
            'div.pl-trustmark::attr(data-brand)',
            'span.product-name-brand::text',
            'span.chanel-brand-name::text'
        ]

        brand = None
        try:
            for brand_selector in brand_selectors:
                brand_string = response.css(brand_selector).get()
                if brand_string:
                    brand = brand_string.strip()
                    break
        except Exception as e:
            brand = None

        try:
            product_name = response.css('span.product-name-title::text').get()
        except Exception as e:
            product_name = None

        try:
            master_sku_id = response.css('div#pdpMain::attr(data-master-product-id)').get()
            miscellaneous['master_sku_id'] = master_sku_id
        except:
            master_sku_id = None

        try:
            product_description = get_product_description(response)
            product_description_clean = product_description.replace('\n','')
        except:
            product_description_clean = None

        size_list = response.css('ul.sizeselector-list li span.sizeselector-item_text::text').getall()
        color_link = response.css('a.colorselector-link::attr(href)').get()

        if size_list:
            for size in size_list:
                size = size.strip()
                variant_url = f'https://www.brownthomas.com/on/demandware.store/Sites-BrownThomas-Site/en_IE/Product-Variation?pid={master_sku_id}&dwvar_{master_sku_id}_size={size}&Quantity=1&format=ajax&productlistid=undefined'

                headers = {
                    'accept': 'text/html, */*; q=0.01',
                    'accept-language': 'en-US,en;q=0.9',
                    'priority': 'u=1, i',
                    'referer': product_url,
                    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                }

                string_response = requests.request("GET", variant_url,headers=headers)
                response_object = Selector(text=string_response.text)

                try:
                    sku_id = response_object.css('input.quantity_selector-input::attr(data-pid)').get()
                except:
                    sku_id = None

                try:
                    oos_string = response_object.css('button[title="Add to Bag"]::text').get()
                    
                    if oos_string.lower() == 'out of stock':
                        oos = 1
                    else:
                        oos = 0

                except:
                    oos = None

                ## for size list there should not be image variations
                try:
                    image_url = f'https://cdn.media.amplience.net/i/bta/{master_sku_id}_01'
                except:
                    image_url = None

                try:
                    price = response_object.css('meta[itemprop="price"]::attr(content)').get()
                except:
                    price = None

                try:
                    mrp = response_object.css('span.price-standard.was-price::text').getall()[-1]
                    mrp = mrp.replace('€','').strip()
                except:
                    mrp = None

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
                item['product_description'] = product_description_clean
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

                yield item

        elif color_link:
            color_url = 'https://www.brownthomas.com' + color_link
            string_response = requests.request("GET", color_url)
            response_object = Selector(text=string_response.text)

            color_codes = response_object.css('img.colorselector-swatch_image::attr(alt)').getall()

            for color_code in color_codes:
                color_code_encode = color_code.replace('-','%20')
                variant_url = f'https://www.brownthomas.com/on/demandware.store/Sites-BrownThomas-Site/en_IE/Product-Variation?pid={master_sku_id}&dwvar_{master_sku_id}_color={color_code_encode}&Quantity=1&format=ajax&productlistid=undefined'

                headers = {
                    'accept': 'text/html, */*; q=0.01',
                    'accept-language': 'en-US,en;q=0.9',
                    'priority': 'u=1, i',
                    'referer': product_url,
                    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                }

                string_response = requests.request("GET", variant_url,headers=headers)
                response_object = Selector(text=string_response.text)

                miscellaneous['color_code'] = color_code

                try:
                    sku_id = response_object.css('input.quantity_selector-input::attr(data-pid)').get()
                except:
                    sku_id = None

                try:
                    oos_string = response_object.css('button[title="Add to Bag"]::text').get()
                    
                    if oos_string.lower() == 'out of stock':
                        oos = 1
                    else:
                        oos = 0

                except:
                    oos = None

                try:
                    image_url = f'https://cdn.media.amplience.net/i/bta/{master_sku_id}_{sku_id}_01'
                except:
                    image_url = None
                
                try:
                    price = response_object.css('meta[itemprop="price"]::attr(content)').get()
                except:
                    price = None

                try:
                    mrp = response_object.css('span.price-standard.was-price::text').getall()[-1]
                    mrp = mrp.replace('€','').strip()
                except:
                    mrp = None

                try:
                    size = get_size_from_title(product_name)
                except:
                    size = None

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
                item['product_description'] = product_description_clean
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

                yield item 
        