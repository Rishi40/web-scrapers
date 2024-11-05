from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
import time
import requests
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_sku_ids, BENCHMARK_DATE, get_web_page, write_to_log
from datetime import datetime
import math
from scrapy import signals

WEBSITE_ID = 16
PRODUCT_LIST_LIMIT = 200

sub_category_cgid_map = {
    'fragrance': '2534374302032078',
    'makeup': '2534374302032105',
    'skin-care': '2534374302032157'
}

def get_payload(page_number,sub_category):
    payload = {
        'fragrance': {
            "requests":
                [
                    {"indexName":"prod_thebay_search_idx_en","params":f"analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=true&facetFilters=%5B%5B%22hierarchicalCategories.1%3ABeauty%20%3E%20Fragrance%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22*%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092270445%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091141248%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600090799433%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600085593819%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092707535%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092860420%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092707526%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092351807%3Cscore%3D10000%3E%22%5D&page={page_number}&query=&ruleContexts=%5B%22TheBay%22%5D&tagFilters="},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetFilters=%5B%5B%22hierarchicalCategories.0%3ABeauty%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%2C%22hierarchicalCategories.1%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092270445%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091141248%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600090799433%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600085593819%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092707535%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092860420%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092707526%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092351807%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D"},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092270445%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091141248%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600090799433%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600085593819%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092707535%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092860420%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092707526%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092351807%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D&facetFilters=undefined"}
                ]
        },
        'makeup': {
            "requests":
                [
                    {"indexName":"prod_thebay_search_idx_en","params":f"analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=true&facetFilters=%5B%5B%22hierarchicalCategories.1%3ABeauty%20%3E%20Makeup%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22*%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092583942%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091974986%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600091481737%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600093278241%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092584261%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092089848%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600093238801%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092301691%3Cscore%3D10000%3E%22%5D&page={page_number}&query=&ruleContexts=%5B%22TheBay%22%5D&tagFilters="},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetFilters=%5B%5B%22hierarchicalCategories.0%3ABeauty%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%2C%22hierarchicalCategories.1%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092583942%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091974986%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600091481737%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600093278241%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092584261%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092089848%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600093238801%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092301691%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D"},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092583942%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091974986%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600091481737%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600093278241%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092584261%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092089848%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600093238801%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600092301691%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D&facetFilters=undefined"}
                ]
        },
        'skin-care': {
            "requests":
                [
                    {"indexName":"prod_thebay_search_idx_en","params":f"analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=true&facetFilters=%5B%5B%22hierarchicalCategories.1%3ABeauty%20%3E%20Skin%20Care%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22*%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092238685%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091637867%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600092827734%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600092827736%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092293107%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092169808%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092896146%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600093100222%3Cscore%3D10000%3E%22%5D&page={page_number}&query=&ruleContexts=%5B%22TheBay%22%5D&tagFilters="},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetFilters=%5B%5B%22hierarchicalCategories.0%3ABeauty%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%2C%22hierarchicalCategories.1%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092238685%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091637867%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600092827734%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600092827736%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092293107%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092169808%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092896146%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600093100222%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D"},
                    {"indexName":"prod_thebay_search_idx_en","params":"analytics=false&analyticsTags=%5B%22TheBay%22%5D&clickAnalytics=false&facetingAfterDistinct=true&facets=%5B%22hierarchicalCategories.0%22%5D&filters=listingSiteTheBay%3Atrue&getRankingInfo=true&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=0&maxValuesPerFacet=5000&optionalFilters=%5B%22variantGroupCode%3A%200600092238685%3Cscore%3D10007%3E%22%2C%22variantGroupCode%3A%200600091637867%3Cscore%3D10006%3E%22%2C%22variantGroupCode%3A%200600092827734%3Cscore%3D10005%3E%22%2C%22variantGroupCode%3A%200600092827736%3Cscore%3D10004%3E%22%2C%22variantGroupCode%3A%200600092293107%3Cscore%3D10003%3E%22%2C%22variantGroupCode%3A%200600092169808%3Cscore%3D10002%3E%22%2C%22variantGroupCode%3A%200600092896146%3Cscore%3D10001%3E%22%2C%22variantGroupCode%3A%200600093100222%3Cscore%3D10000%3E%22%5D&page=0&query=&ruleContexts=%5B%22TheBay%22%5D&facetFilters=undefined"}
                ]
        }
    }

    return payload.get(sub_category)

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
    name = "scrape_the_bay_new"
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

        catalogue_url = "https://q8jszsernp-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (4.14.2); Browser (lite); instantsearch.js (4.54.0); JS Helper (3.11.3)&x-algolia-api-key=e68ea4efad84c823b6fa29c2bf23fed1&x-algolia-application-id=Q8JSZSERNP"
        sub_category_payload = get_payload(0,sub_category)
        visited_sku_list = visited_sku_ids(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        web_pages_list = collect_web_pages(sub_category)

        yield scrapy.Request(
            method="POST",
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            body=json.dumps(sub_category_payload),
            meta={'sub_category':sub_category,'visited_sku_list':visited_sku_list, 'web_pages_list': web_pages_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.thebay.com/product/tom-ford-bitter-peach-0600092333083.html'
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
        sub_category = response.meta['sub_category']
        visited_sku_list = response.meta['visited_sku_list']
        web_pages_list = response.meta['web_pages_list']

        # try:
        #     page_details_string = response.body
        #     page_details_json = json.loads(page_details_string)
        # except:
        #     page_details_json = {}
        
        # if page_details_json:

        #     product_count = page_details_json.get('results')[0].get('nbHits')
        #     print("====PRODUCT COUNT====",product_count)

        #     no_of_pages = page_details_json.get('results')[0].get('nbPages')
        #     print("====no_of_pages====",no_of_pages)
            

        for web_pages in web_pages_list:
            page_start = web_pages[1]
            page_end = web_pages[2]

            for page_index in range(page_start,page_end+1):
                catalogue_url = "https://q8jszsernp-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia for JavaScript (4.14.2); Browser (lite); instantsearch.js (4.54.0); JS Helper (3.11.3)&x-algolia-api-key=e68ea4efad84c823b6fa29c2bf23fed1&x-algolia-application-id=Q8JSZSERNP"
                sub_category_payload = get_payload(page_index,sub_category)

                yield scrapy.Request(
                    method="POST",
                    url=catalogue_url, 
                    callback=self.parse_catalogue_links, 
                    body=json.dumps(sub_category_payload),
                    meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):

        page_index = response.meta['page_index']
        visited_sku_list = response.meta['visited_sku_list']

        try:
            single_page_details_string = response.body
            single_page_details_json = json.loads(single_page_details_string)
        except:
            single_page_details_json = {}
        
        if single_page_details_json:

            products = single_page_details_json.get('results')[0].get('hits')
            query_id = single_page_details_json.get('results')[0].get('queryID')

            for product in products:
                object_id = product.get('objectID')
                product_id = product.get('productId')
                product_name = product.get('displayName')

                product_link = f'https://www.thebay.com/product/show?pid={product_id}&queryID={query_id}&objectID={object_id}'
                metadata = {}
                metadata['product_url'] = product_link
                metadata['page_index'] = page_index
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category

                final_url = SCRAPER_URL + product_link
                # print(product_name)
                if product_id not in visited_sku_list:
                    yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata}, dont_filter=True)
                else:
                    print("------PRODUCT EXISTS------")

        else:
            print("------NO PRODUCTS FOUND------")

    def parse_product(self, response):

        item = FactiveItem()
        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            string_data = response.css('script[type="application/ld+json"]::text').get().strip()
            json_data = json.loads(string_data)
        except:
            json_data = {}

        try:
            brand = response.css('a.product-brand::text').get().strip()
        except Exception as e:
            brand = None

        try:
            product_name = response.css('h1.product-name::text').get().strip()
        except Exception as e:
            product_name = None

        try:
            sku_string = response.css('div.product-detail-id::text').get().strip()
            master_sku_id = sku_string.split(':')[1].strip()
            miscellaneous['master_sku_id'] = master_sku_id
        except:
            master_sku_id = None

        options = response.css('ul[role="radiogroup"]') or response.css('ul.size-attribute') or response.css('select.select-color')

        size_options = [
            'ul[role="radiogroup"] li::attr(data-attr-value)',
            'ul.size-attribute li::attr(data-attr-value)'
        ]

        color_options = [
            'ul[role="radiogroup"] li button::attr(aria-describedby)',
            'select.select-color option::attr(data-attr-value)',
            'span.color span::attr(data-adobelaunchproductcolor)'
        ]

        if options:
            size_list,color_list = [],[]
            for option in size_options:
                size_list = response.css(option).getall()
                if size_list:
                    break

            for option in color_options:
                color_list = response.css(option).getall()
                if color_list:
                    break    

            if size_list and color_list:
                # print("BOTH SIZE AND COLOR")
                for size in size_list:
                    for color in color_list:
                        variant_url = f'https://www.thebay.com/on/demandware.store/Sites-TheBay-Site/en_CA/Product-Variation?dwvar_{master_sku_id}_color={color}&dwvar_{master_sku_id}_size={size}&pid={master_sku_id}&quantity=1&preselect=&makeEstimatedEarnsCall=true'

                        headers = {
                        'authority': 'www.thebay.com',
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                        'x-requested-with': 'XMLHttpRequest'
                        }

                        string_response = requests.request("GET", variant_url,headers=headers)
                        json_response = json.loads(string_response.text)

                        sku_id = json_response.get('product').get('id')
                        oos_string = json_response.get('product').get('available')
                        
                        if oos_string:
                            oos = 0
                        else:
                            oos = 1

                        image_url_list = json_response.get('product').get('images').get('large')[0]
                        image_url = image_url_list.get('url')
                        product_description = json_response.get('product').get('longDescription')
                        product_description_clean = clean_product_description(product_description)

                        try:
                            product_url = json_response.get('product').get('pdpURL')
                        except:
                            product_url = None

                        try:
                            mrp = json_response.get('product').get('price').get('list').get('value')
                        except:
                            mrp = None
                        
                        try:
                            price = json_response.get('product').get('price').get('sales').get('value')
                        except:
                            price = None

                        try:
                            discount = json_response.get('product').get('price').get('savings')
                            rounded_discount = round(discount,2)
                        except:
                            rounded_discount = None

                        rating = json_response.get('product').get('starRating')
                        miscellaneous['rating'] = rating
                        reviews = json_response.get('product').get('turntoReviewCount')
                        miscellaneous['reviews'] = reviews
                        miscellaneous['color'] = color
                        miscellaneous['plain_sku_id'] = sku_id

                        miscellaneous_string = json.dumps(miscellaneous)

                        item['website_id'] = WEBSITE_ID
                        item['scrape_date'] = scrape_date
                        item['category'] = metadata['category']
                        item['sub_category'] = metadata['sub_category']
                        item['brand'] = brand
                        item['sku_id'] = sku_id + '_' + color
                        item['product_name'] = product_name
                        item['product_url'] = product_url
                        item['image_url'] = image_url
                        item['product_description'] = product_description_clean
                        item['info_table'] = None
                        item['out_of_stock'] = oos
                        item['price'] = price
                        item['mrp'] = mrp
                        item['high_street_price'] = None
                        item['discount'] = rounded_discount
                        item['size'] = size
                        item['qty_left'] = None
                        item['usd_price'] = None
                        item['usd_mrp'] = None
                        item['miscellaneous'] = miscellaneous_string

                        yield item

            elif size_list and not color_list:
                # print("ONLY SIZE")
                for size in size_list:
                    variant_url = f'https://www.thebay.com/on/demandware.store/Sites-TheBay-Site/en_CA/Product-Variation?dwvar_{master_sku_id}_color=NO_COLOUR&dwvar_{master_sku_id}_size={size}&pid={master_sku_id}&quantity=1&preselect=&makeEstimatedEarnsCall=true'

                    headers = {
                    'authority': 'www.thebay.com',
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                    }

                    string_response = requests.request("GET", variant_url,headers=headers)
                    json_response = json.loads(string_response.text)

                    sku_id = json_response.get('product').get('id')
                    oos_string = json_response.get('product').get('available')
                    
                    if oos_string:
                        oos = 0
                    else:
                        oos = 1

                    image_url_list = json_response.get('product').get('images').get('large')[0]
                    image_url = image_url_list.get('url')
                    product_description = json_response.get('product').get('longDescription')
                    product_description_clean = clean_product_description(product_description)

                    try:
                        product_url = json_response.get('product').get('pdpURL')
                    except:
                        product_url = None

                    try:
                        mrp = json_response.get('product').get('price').get('list').get('value')
                    except:
                        mrp = None
                    
                    try:
                        price = json_response.get('product').get('price').get('sales').get('value')
                    except:
                        price = None

                    try:
                        discount = json_response.get('product').get('price').get('savings')
                        rounded_discount = round(discount,2)
                    except:
                        rounded_discount = None

                    rating = json_response.get('product').get('starRating')
                    miscellaneous['rating'] = rating
                    reviews = json_response.get('product').get('turntoReviewCount')
                    miscellaneous['reviews'] = reviews

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
                    item['discount'] = rounded_discount
                    item['size'] = size
                    item['qty_left'] = None
                    item['usd_price'] = None
                    item['usd_mrp'] = None
                    item['miscellaneous'] = miscellaneous_string

                    yield item

            elif color_list and not size_list:
                # print("ONLY COLOR")
                size = response.css('span.text2::text').getall()[-1]
                for color in color_list:
                    variant_url = f'https://www.thebay.com/on/demandware.store/Sites-TheBay-Site/en_CA/Product-Variation?dwvar_{master_sku_id}_color={color}&dwvar_{master_sku_id}_size={size}&pid={master_sku_id}&quantity=1&preselect={color}&makeEstimatedEarnsCall=true'

                    headers = {
                        'authority': 'www.thebay.com',
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                        'x-requested-with': 'XMLHttpRequest'
                    }

                    string_response = requests.request("GET", variant_url,headers=headers)
                    json_response = json.loads(string_response.text)

                    sku_id = json_response.get('product').get('id')
                    availability_string = json_response.get('product').get('available')
                    
                    if availability_string:
                        oos = 0
                    else:
                        oos = 1

                    image_url_list = json_response.get('product').get('images').get('large')[0]
                    image_url = image_url_list.get('url')
                    product_description = json_response.get('product').get('longDescription')
                    product_description_clean = clean_product_description(product_description)

                    try:
                        product_url = json_response.get('product').get('pdpURL')
                    except:
                        product_url = None

                    try:
                        mrp = json_response.get('product').get('price').get('list').get('value')
                    except:
                        mrp = None
                    
                    try:
                        price = json_response.get('product').get('price').get('sales').get('value')
                    except:
                        price = None

                    try:
                        discount = json_response.get('product').get('price').get('savings')
                        rounded_discount = round(discount,2)
                    except:
                        rounded_discount = None

                    rating = json_response.get('product').get('starRating')
                    miscellaneous['rating'] = rating
                    reviews = json_response.get('product').get('turntoReviewCount')
                    miscellaneous['reviews'] = reviews
                    miscellaneous['color'] = color

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
                    item['discount'] = rounded_discount
                    item['size'] = size
                    item['qty_left'] = None
                    item['usd_price'] = None
                    item['usd_mrp'] = None
                    item['miscellaneous'] = miscellaneous_string

                    yield item 

        else:
            # print("NONE")
            if json_data:
                try:
                    product_description = json_data.get('description')
                    product_description_clean = clean_product_description(product_description)
                except:
                    product_description_clean = ''

                try:
                    sku_id = json_data.get('sku')
                except:
                    sku_id = ''

                try:
                    rating = json_data.get('aggregateRating').get('ratingValue')
                    miscellaneous['rating'] = rating
                except:
                    rating = ''

                try:
                    reviews = json_data.get('aggregateRating').get('reviewCount')
                    miscellaneous['reviews'] = reviews
                except:
                    reviews = ''

            else:
                product_description_clean = ''
                sku_id = ''

            oos = 0   #oos = to be decided

            try:
                product_url = response.css('input#shareUrl::attr(value)').get() 
            except:
                product_url = None

            try:
                image_url = response.css('div#primary-image-0 img::attr(src)').get()
            except Exception as e:
                image_url = None

            try:
                size = response.css('span.text2::text').getall()[-1]
            except Exception as e:
                size = ''

            try:
                price = response.css('span.formatted_sale_price::text').get().replace('$','')
            except:
                price = None

            try:
                mrp = response.css('span.strike-through span.formatted_price::text').get().strip().replace('$','')
            except:
                mrp = None

            try:
                discount = response.css('span.formatted-savings::text').get().replace('\n','')
            except:
                discount = None

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
            item['discount'] = discount
            item['size'] = size
            item['qty_left'] = None
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = miscellaneous_string

            yield item