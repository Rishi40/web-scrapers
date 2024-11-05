from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import requests
from worldduty.items import CrawlerItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
import math
import urllib.parse
from scrapy import signals

WEBSITE_ID = 58
PRODUCT_LIST_LIMIT = 24

sub_category_code_dict = {
    'fragrance': 'FRAGRANCE-C301',
    'make-up': 'MAKEUP-C302',
    'skincare': 'SKINCARE-C303',
    'bath-body': 'BATH-%26-BODY-C304'
}

def get_product_description(response):
    product_description_raw_list = []

    main_description_list = [
        'div.product-description-box p::text',
        'div.product-description-box span::text',
        'div.product-description-box div::text',
    ]

    for md in main_description_list:
        desc_list = response.css(md).getall()
        product_description_raw_list.append(desc_list)

    product_description_list = [item for pd in product_description_raw_list for item in pd]
    product_description = ' '.join(product_description_list)

    return product_description
    
class WdcSpider(scrapy.Spider):
    name = "scrape_sephora_sa"
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
        second_day_of_current_month = datetime.today().replace(day=1).strftime("%Y-%m-%d")
        sub_category_code = sub_category_code_dict[sub_category]
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = f'https://www.sephora.sa/en/Shop/{sub_category_code}/?listview=true'

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={'sub_category_code':sub_category_code,'visited_sku_list':visited_sku_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.sephora.bh/bh/en/p/surrealskin%E2%84%A2-foundation-P10051674.html'
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

        sub_category_code = response.meta['sub_category_code']
        visited_sku_list = response.meta['visited_sku_list']

        try:
            product_count_string = response.css('label.show-results::text').get()
            product_count = product_count_string.split()[1]
            product_count = product_count.replace('(','').replace(')','')

        except:
            print("Failed")
            product_count = 0

        print("====PRODUCT COUNT====",product_count)
        no_of_pages = int(product_count)//PRODUCT_LIST_LIMIT
        offset = 0

        # no_of_pages = 2 ## For Testing
        for page_index in range(1,no_of_pages+1):
            url = f'https://www.sephora.sa/en/Shop/{sub_category_code}/?srule=General%20sorting%20rule&start={offset}&sz={PRODUCT_LIST_LIMIT}&format=page-element&on=onclickload'
            catalogue_links_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=catalogue_links_url, 
                callback=self.parse_catalogue_links, 
                meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                dont_filter=True)

            offset = offset + PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):

        product_cards = response.css('li.grid-tile')
        visited_sku_list = response.meta['visited_sku_list']

        if product_cards:
            for product in product_cards:
                product_link = product.css('a.product-tile-link::attr(href)').get() 

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category

                final_url = SCRAPER_URL + product_link
                if product_link not in visited_sku_list:
                    # print(product_link)
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata}, dont_filter=True)
                else:
                    print("--------PRODUCT EXISTS--------")

    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            brand = response.css('span.brand-name a::text').get().strip()
        except Exception as e:
            try:
                brand = response.css('span.brand-name::text').get().strip()
            except:
                brand = None

        try:
            product_name = response.css('span.product-name::text').get().strip()
        except Exception as e:
            product_name = None

        try:
            product_description = get_product_description(response)
            product_description_clean = product_description.replace('\n','')
        except:
            product_description_clean = None

        # try:
        #     ingredients_list = response.css('div.ingredients-content p::text').getall()
        #     ingredients = ' '.join(ingredients_list).strip()

        #     if ingredients:
        #         miscellaneous['ingredients'] = ingredients

        # except:
        #     ingredients = None

        try:
            olfac_notes_list = response.css('div.pdp-notes-contents p::text').getall()
            olfac_notes = ' '.join(olfac_notes_list).strip()

            if olfac_notes:
                miscellaneous['olfac_notes'] = olfac_notes
        except:
            olfac_notes = None

        # try:
        #     mode_of_use_list = response.css('div.tips-content p::text').getall()
        #     mode_of_use = ' '.join(mode_of_use_list).strip()

        #     if mode_of_use:
        #         miscellaneous['mode_of_use'] = mode_of_use

        # except:
        #     mode_of_use = None

        try:
            rating = response.css('span[itemprop="ratingValue"]::text').get().strip()
            if rating:
                miscellaneous['rating'] = rating
        except:
            rating = None

        try:
            reviews = response.css('span.bv_numReviews_text::text').get().strip()
            if reviews:
                miscellaneous['reviews'] = reviews
        except:
            reviews = None


        variation_object_list = response.css('button.variation-button') 

        if variation_object_list:
            for i,variant_object in enumerate(variation_object_list):

                try:
                    variation_name = variant_object.css('::attr(title)').get()
                    final_product_name = product_name + ' ' + variation_name
                    miscellaneous['variant_name'] = variation_name
                except Exception as e:
                    variation_name = None

                try:
                    size = get_size_from_title(final_product_name)
                except:
                    size = None
                
                try:
                    image_url_object = response.css('a.variation-display-name::attr(data-lgimg)').getall()[i]               
                    image_url = json.loads(image_url_object).get('url') 
                except:
                    image_url = None

                try:
                    sku_id = variant_object.css('::attr(data-pid)').get()
                except:
                    sku_id = None

                try:
                    price = variant_object.css('span.price-sales::text').get()
                    price = price.replace('SAR','').replace(',','.').strip()
                except:
                    price = None

                try:
                    mrp = variant_object.css('span.price-standard::text').get()
                    mrp = mrp.replace('SAR','').replace(',','.').strip()
                except:
                    mrp = None

                try:
                    discount = variant_object.css('span.original-price-discount::text').get()
                    discount = discount.replace('-','').strip()
                except:
                    discount = None

                try:
                    oos_string = variant_object.css('span.variation-avaibility::text').get().strip()
                    if oos_string.lower() != 'available' or price == 'N/A':
                        oos = 1
                    else:
                        oos = 0
                except:
                    oos = None

                miscellaneous_string = json.dumps(miscellaneous)

                item['website_id'] = WEBSITE_ID
                item['scrape_date'] = scrape_date
                item['category'] = metadata['category']
                item['sub_category'] = metadata['sub_category']
                item['brand'] = brand
                item['sku_id'] = sku_id
                item['product_name'] = final_product_name
                item['product_url'] = product_url
                item['image_url'] = image_url
                item['product_description'] = product_description_clean
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
