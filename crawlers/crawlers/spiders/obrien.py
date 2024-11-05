from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
import time
from scrapy import signals

WEBSITE_ID = 28

def get_payload(page_number,sub_category):
    payload = {
        'spirits' : {
            "requests":
                [
                    {
                        "indexName":"online_products",
                        "params":f"clickAnalytics=true&distinct=true&facetingAfterDistinct=true&facets=%5B%22named_tags.Product%20group%22%2C%22named_tags.BRAND%22%2C%22named_tags.Style%22%2C%22named_tags.Country%22%2C%22named_tags.FEATURES%22%2C%22named_tags.type%22%2C%22named_tags.colour%22%2C%22named_tags.country%22%2C%22named_tags.region%22%2C%22price_range%22%2C%22named_tags.ABV%20%25%22%5D&filters=collection_ids%3A%22135218462798%22%20AND%20inventory_quantity%20%3E%200&highlightPostTag=%3C%2Fspan%3E&highlightPreTag=%3Cspan%20class%3D%22ais-highlight%22%3E&hitsPerPage=30&maxValuesPerFacet=10&page={page_number}&query=&ruleContexts=%5B%22spirits%22%5D&tagFilters="
                    }
                ]
        },
        'beer': {
            "requests":
                [
                    {
                        "indexName":"online_products",
                        "params":f"clickAnalytics=true&distinct=true&facetingAfterDistinct=true&facets=%5B%22named_tags.Product%20group%22%2C%22named_tags.Style%22%2C%22named_tags.GRAPE%20TYPE%22%2C%22named_tags.Country%22%2C%22named_tags.Region%22%2C%22named_tags.WINEMAKER%22%2C%22named_tags.Producer%22%2C%22named_tags.FEATURES%22%2C%22named_tags.type%22%2C%22named_tags.colour%22%2C%22named_tags.country%22%2C%22named_tags.region%22%2C%22named_tags.food%20match%22%2C%22named_tags.wineries%22%2C%22price_range%22%2C%22named_tags.ABV%20%25%22%5D&filters=collection_ids%3A%22135218233422%22%20AND%20inventory_quantity%20%3E%200&highlightPostTag=%3C%2Fspan%3E&highlightPreTag=%3Cspan%20class%3D%22ais-highlight%22%3E&hitsPerPage=30&maxValuesPerFacet=10&page={page_number}&query=&ruleContexts=%5B%22beer%22%5D&tagFilters="
                    }
                ]
        },
        'wine': {
            "requests":
                [
                    {
                        "indexName":"online_products",
                        "params":f"clickAnalytics=true&distinct=true&facetingAfterDistinct=true&facets=%5B%22named_tags.Product%20group%22%2C%22named_tags.Style%22%2C%22named_tags.GRAPE%20TYPE%22%2C%22named_tags.Country%22%2C%22named_tags.Region%22%2C%22named_tags.WINEMAKER%22%2C%22named_tags.Producer%22%2C%22named_tags.FEATURES%22%2C%22named_tags.type%22%2C%22named_tags.colour%22%2C%22named_tags.country%22%2C%22named_tags.region%22%2C%22named_tags.food%20match%22%2C%22named_tags.wineries%22%2C%22price_range%22%2C%22named_tags.ABV%20%25%22%5D&filters=collection_ids%3A%22135203782734%22%20AND%20inventory_quantity%20%3E%200&highlightPostTag=%3C%2Fspan%3E&highlightPreTag=%3Cspan%20class%3D%22ais-highlight%22%3E&hitsPerPage=30&maxValuesPerFacet=10&page={page_number}&query=&ruleContexts=%5B%22wine%22%5D&tagFilters="
                    }
                ]
        },
        'champange-sparkling': {
            "requests":
                [
                    {
                        "indexName":"online_products",
                        "params":f"clickAnalytics=true&distinct=true&facetingAfterDistinct=true&facets=%5B%22named_tags.Product%20group%22%2C%22named_tags.Style%22%2C%22named_tags.GRAPE%20TYPE%22%2C%22named_tags.Country%22%2C%22named_tags.Region%22%2C%22named_tags.WINEMAKER%22%2C%22named_tags.Producer%22%2C%22named_tags.FEATURES%22%2C%22named_tags.type%22%2C%22named_tags.colour%22%2C%22named_tags.country%22%2C%22named_tags.region%22%2C%22named_tags.food%20match%22%2C%22named_tags.wineries%22%2C%22price_range%22%2C%22named_tags.ABV%20%25%22%5D&filters=collection_ids%3A%22161159675982%22%20AND%20inventory_quantity%20%3E%200&highlightPostTag=%3C%2Fspan%3E&highlightPreTag=%3Cspan%20class%3D%22ais-highlight%22%3E&hitsPerPage=30&maxValuesPerFacet=10&page={page_number}&query=&ruleContexts=%5B%22champange-sparkling%22%5D&tagFilters="
                    }
                ]
        }
    }

    return payload.get(sub_category)

class WdcSpider(scrapy.Spider):
    name = "scrape_obrien"
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
 
        second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = 'https://c8uhhfns0x-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (4.14.2); Browser (lite); instantsearch.js (4.49.1); JS Helper (3.11.1)&x-algolia-api-key=3b226d41f1b6abee1cf1531006ae8f8f&x-algolia-application-id=C8UHHFNS0X'
        sub_category_payload = get_payload(1,sub_category)

        yield scrapy.Request(
            method="POST",
            url=url, 
            body=json.dumps(sub_category_payload),
            callback=self.parse_catalogue_pages,
            meta = {'visited_sku_list':visited_sku_list,'sub_category':sub_category},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.obrienswine.ie/products/wicklow-wolf-mammoth-ipa-44cl-can'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['qty_left'] = 10

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
        sub_category = response.meta['sub_category']

        try:
            string_data = response.body
            json_data = json.loads(string_data)
        except:
            json_data = {}

        if json_data:
            no_of_pages = json_data.get('results')[0].get('nbPages')
            print("NO OF PAGES ====", no_of_pages)

            # no_of_pages = 1 ## For Testing
            for page_index in range(no_of_pages):
                url = 'https://c8uhhfns0x-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (4.14.2); Browser (lite); instantsearch.js (4.49.1); JS Helper (3.11.1)&x-algolia-api-key=3b226d41f1b6abee1cf1531006ae8f8f&x-algolia-application-id=C8UHHFNS0X'
                sub_category_payload = get_payload(page_index,sub_category)

                yield scrapy.Request(
                    method="POST",
                    url=url, 
                    body=json.dumps(sub_category_payload),
                    callback=self.parse_catalogue_links,
                    meta = {'visited_sku_list':visited_sku_list},
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']

        try:
            string_data = response.body
            json_data = json.loads(string_data)
        except:
            json_data = {}

        if json_data:
            products = json_data.get('results')[0].get('hits')
            for product in products:
                qty_left = product.get('inventory_quantity')
                product_end_point = product.get('handle')
                product_link = 'https://www.obrienswine.ie/products/' + product_end_point

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category
                metadata['qty_left'] = qty_left

                final_url = SCRAPER_URL + product_link 

                if product_link not in visited_sku_list:
                    yield scrapy.Request(
                        url=final_url, 
                        callback=self.parse_product,
                        meta={'metadata':metadata}
                    )
        else:
            print("NO PRODUCTS FOUND")

    def parse_product(self, response):

        item = CrawlerItem()
        scrape_date = datetime.today().strftime('%Y-%m-%d')
        miscellaneous = {}
        metadata = response.meta['metadata']
        product_url = metadata['product_url']
        qty_left = metadata['qty_left']

        try:
            string_data = response.css('script[type="application/ld+json"]::text').getall()[-1]
            string_data_clean = string_data.strip()
            json_data = json.loads(string_data_clean)
        except:
            json_data = {}

        if json_data:
            try:
                brand = None
                product_details_object = None
                script_text = response.css('script#swym-snippet::text').get().strip()
                script_list = script_text.split(';')  
                for script_string in script_list:
                    if 'handle' in script_string.lower():
                        product_details_object = script_string
                
                if product_details_object:
                    product_details_object_clean = product_details_object.replace('window.SwymProductInfo.product = ','').strip()
                    product_details_json = json.loads(product_details_object_clean)
                    product_tags_list = product_details_json.get('tags')
                    for tag in product_tags_list:
                        if 'brand' in tag.lower():
                            brand = tag.split(':')[1]
            except:
                brand = None

            try:
                if not brand:
                    brand = json_data.get('brand').get('name') 
            except:
                brand = None

            try:
                sku_id = json_data.get('sku') 
            except:
                sku_id = None

            try:
                product_url = json_data.get('url')
            except Exception as e:
                product_url = ''
                
            try:
                product_name = json_data.get('name')
            except Exception as e:
                product_name = ''

            try:
                image_url = json_data.get('image')[0]
            except Exception as e:
                image_url = ''

            try:
                product_description = json_data.get('description')
            except Exception as e:
                product_description = ''
            
            try:
                price = json_data.get('offers')[0].get('price') 
            except:
                price = None

            try:
                oos_string = json_data.get('offers')[0].get('availability') 
                if 'instock' in oos_string.lower():
                    oos = 0
                else:
                    oos = 1
            except:
                oos = None
        
            try:
                size = get_size_from_title(product_name)
            except:
                size = None

            try:
                mrp = response.css('s.price-item--regular::text').get().strip().replace('€','')
            except:
                mrp = None

            # try:
            #     discount_list = response.css('div.medium-hide div.product__highlights-item::text').getall()
            #     discount_list_clean = [discount_item.strip() for discount_item in discount_list] 
            #     discount = ''.join(discount_list_clean) 
            # except:
            #     discount = None

            try:
                country = response.css('p.product__subheading.medium-hide::text').get().strip().replace('\n','').replace(' ','') 
                miscellaneous['country'] = country
            except:
                country = None

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
            item['discount'] = None
            item['size'] = size
            item['qty_left'] = qty_left
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = miscellaneous_string

            yield item

