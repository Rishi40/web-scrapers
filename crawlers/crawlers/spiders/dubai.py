from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import get_size_from_title, BENCHMARK_DATE, SCRAPE_DATE, visited_sku_ids, write_to_log
import datetime
from scrapy import signals

WEBSITE_ID = 2
VISIT_ID = '-48c7788a%3A18c8c94743b%3A-1656-4094298370'
VISITOR_ID = '115Ewa7gDn1F-TvHeQqTykoWnkJHEZvW-hiCuPtJoXpFZEI08C1'
PRODUCT_LIST_LIMIT = 250

CATALOG_HEADERS = {
    'authority': 'www.dubaidutyfree.com',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-cc-meteringmode': 'CC-NonMetered',
    'x-ccasset-language': 'en',
    'x-ccpricelistgroup': 'aED',
    'x-ccprofiletype': 'storefrontUI',
    'x-ccsite': 'siteUS',
    'x-ccviewport': 'lg',
    'x-ccvisitid': VISIT_ID,
    'x-ccvisitorid': VISITOR_ID,
    'x-requested-with': 'XMLHttpRequest'
}

PRODUCT_HEADERS = {
    'authority': 'www.dubaidutyfree.com',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'if-none-match': '"eyJ2ZXJzaW9uIjowLCJ1cmkiOiJwYWdlcy9kaW9yLXNhdXZhZ2UtcGFyZnVtLTEwMG1sL3Byb2R1Y3QvMTExNDEzOTQzIiwiaGFzaCI6IkdxUFZIdz09In0="',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-cc-meteringmode': 'CC-NonMetered',
    'x-ccpricelistgroup': 'aED',
    'x-ccprofiletype': 'storefrontUI',
    'x-ccsite': 'siteUS',
    'x-ccviewport': 'lg',
    'x-ccvisitorid': VISITOR_ID,
    'x-requested-with': 'XMLHttpRequest'
}

STOCK_HEADERS = {
  'authority': 'www.dubaidutyfree.com',
  'accept': 'application/json, text/javascript, */*; q=0.01',
  'accept-language': 'en-US,en;q=0.9',
  'content-type': 'application/json',
  'if-none-match': '"eyJ2ZXJzaW9uIjowLCJ1cmkiOiJzdG9ja1N0YXR1cy8xMTE0MTM5NDMiLCJoYXNoIjoiamxUVW53PT0ifQ=="',
  'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'x-cc-meteringmode': 'CC-NonMetered',
  'x-ccasset-language': 'en',
  'x-ccpricelistgroup': 'aED',
  'x-ccprofiletype': 'storefrontUI',
  'x-ccsite': 'siteUS',
  'x-ccviewport': 'lg',
  'x-ccvisitorid': VISITOR_ID,
  'x-requested-with': 'XMLHttpRequest'
}

sub_category_id_map = {
    'perfumes': '599645672',
    'make-up': '2943922374',
    'skincare': '4193966496',
    'liquor': '1213149793',
    'food': '1731201277',
    'tobacco': '1915932559',
    'jewellery': '565695998',
    'sunglasses': '4118342267'
}

def clean_name(name):
    cleaned_name = name.replace('<p>','').replace('</p>','').replace('<br />','').replace('<br >','').replace("&rsquo;","'").replace('&reg;','®').replace("&#39;","'").replace("&nbsp;"," ").replace('&ldquo;','"').replace('&rdquo;','"').replace('&eacute;','é').replace('&quot;','"').replace('&lt;p&gt;','').replace('&lt;/p&gt;','').replace('&amp;','&').replace("&#39;","'").replace('&lt;','').replace('&gt;','').replace('strong','').replace('/strong','').replace('/',' ')
    return cleaned_name

class DubaiSpider(scrapy.Spider):
    name = "scrape_dubai_duty_free"
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
        sub_category_id = sub_category_id_map[sub_category]
        visited_sku_list = visited_sku_ids(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = 'https://www.dubaidutyfree.com/ccstorex/custom/v1/getexchangerate/ONLINE'
        yield scrapy.Request(
            url= url, 
            callback=self.parse_exchange_rate,
            meta = {
                'category':category,
                'sub_category':sub_category,
                'sub_category_id':sub_category_id,
                'visited_sku_list': visited_sku_list
            },
            dont_filter=True
        )

        ## For Testing

        # slug = 'loréal-paris-volume-million-lashes-so-couture-black/product/110318092'
        # url = f'https://www.dubaidutyfree.com/ccstoreui/v1/pages/{slug}?dataOnly=false&cacheableDataOnly=true&productTypesRequired=true'

        # yield scrapy.Request(
        #     url= url, 
        #     callback=self.parse_product,
        #     meta = {
        #         'category':'food',
        #         'sub_category':'food',
        #         'days_available':100,
        #         'exchange_rate': 3.6
        #     },
        #     dont_filter=True
        # )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DubaiSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        stats = spider.crawler.stats.get_stats()
        spider.crawler.stats.set_value('sub_category', self.sub_category)
        log_file_name = f"{spider.name}_log.txt"
        write_to_log(log_file_name,spider.name,stats)

    def parse_exchange_rate(self,response):
        category = response.meta['category']
        sub_category = response.meta['sub_category']
        sub_category_id = response.meta['sub_category_id']
        visited_sku_list = response.meta['visited_sku_list']

        json_data = json.loads(response.body)
        currency_exchange_data = json_data['data']
        aed_usd_conversion = 0

        for c in currency_exchange_data:
            if c['symbol'].lower() == 'usd':
                aed_usd_conversion = float(c['CURRENCYRATE'])

        if not aed_usd_conversion:
            aed_usd_conversion = 3.6

        url = f'https://www.dubaidutyfree.com/ccstoreui/v1/search?N={sub_category_id}&searchType=simple&No=0&Nrpp={PRODUCT_LIST_LIMIT}&Nr=product.x_availableInCatalog%3Aonline&visitorId={VISITOR_ID}&visitId={VISIT_ID}&language=undefined'
        yield scrapy.Request(
            url= url, 
            callback=self.parse_catalogue_pages,
            headers=CATALOG_HEADERS,
            meta = {
                'category':category,
                'sub_category':sub_category,
                'sub_category_id':sub_category_id,
                'exchange_rate': aed_usd_conversion,
                'visited_sku_list': visited_sku_list
            },
            dont_filter=True
        )

    def parse_catalogue_pages(self, response):

        category = response.meta['category']
        sub_category = response.meta['sub_category']
        sub_category_id = response.meta['sub_category_id']
        exchange_rate = response.meta['exchange_rate']
        visited_sku_list = response.meta['visited_sku_list']

        json_data = json.loads(response.body)
        product_count = json_data['resultsList']['totalNumRecs']

        print("====PRODUCT COUNT====",product_count)
        no_of_pages = int(product_count)//PRODUCT_LIST_LIMIT + 1
        products_index = 0

        # no_of_pages = 1 ## For Testing
        
        for page_index in range(no_of_pages):
            catalogue_api_url = f'https://www.dubaidutyfree.com/ccstoreui/v1/search?N={sub_category_id}&searchType=simple&No={products_index}&Nrpp={PRODUCT_LIST_LIMIT}&Nr=product.x_availableInCatalog%3Aonline&visitorId={VISITOR_ID}&visitId={VISIT_ID}&language=undefined'

            # print("CATALOGUE API URL==================",catalogue_api_url)
            yield scrapy.Request(
                url=catalogue_api_url, 
                callback=self.parse_catalogue_links, 
                headers=CATALOG_HEADERS,
                meta = {
                    'category':category,
                    'sub_category':sub_category,
                    'sub_category_id':sub_category_id,
                    'exchange_rate': exchange_rate,
                    'visited_sku_list': visited_sku_list
                },
                dont_filter=True)

            products_index += PRODUCT_LIST_LIMIT

            ## incase product index is not started from zero, it should still break at the relevant point
            if products_index > product_count:
                break

    def parse_catalogue_links(self, response):
        category = response.meta['category']
        sub_category = response.meta['sub_category']
        sub_category_id = response.meta['sub_category_id']
        exchange_rate = response.meta['exchange_rate']
        visited_sku_list = response.meta['visited_sku_list']

        json_data = json.loads(response.body)
        products = json_data['resultsList']['records']

        if not products:
            print("PRODUCTS DO NOT EXIST")
            return
        
        for i in range(len(products)):
            product = products[i]['records'][0]['attributes']
            days_available = product['product.daysAvailable'][0]
            route = product['product.route'][0]
            product_id = product['product.id'][0]

            if product_id not in visited_sku_list:
                product_api_url = 'https://www.dubaidutyfree.com/ccstoreui/v1/pages'+ route + '?dataOnly=false&cacheableDataOnly=true&productTypesRequired=true'

                yield scrapy.Request(
                    url= product_api_url, 
                    callback=self.parse_product,
                    headers=PRODUCT_HEADERS,
                    meta = {
                        'category':category,
                        'sub_category':sub_category,
                        'days_available':days_available,
                        'exchange_rate': exchange_rate
                    },
                    dont_filter=True
                )
            else:
                print("-------PRODUCT EXISTS-------")

    def parse_product(self, response):

        category = response.meta['category']
        sub_category = response.meta['sub_category']
        days_available = response.meta['days_available']
        exchange_rate = response.meta['exchange_rate']

        dubaiItem = FactiveItem()

        dubaiItem['category'] = category
        dubaiItem['sub_category'] = sub_category

        json_data = json.loads(response.body)
        base_data = json_data['data']['page']['product']

        try:
            brand = base_data['brand']
        except Exception as e:
            brand = ''

        try:
            product_name = base_data['displayName']
        except Exception as e:
            product_name = ''

        try:
            mrp = base_data['listPrice']
        except:
            mrp = None

        try:
            price = base_data['salePrice']
        except:
            price = None

        if not price:
            price = mrp

        try:
            usd_mrp_float = mrp/exchange_rate
            usd_mrp_decimal = re.search('[0-9]?[0-9]?\.([0-9][0-9]?)?',str(usd_mrp_float)).group(1)
            usd_mrp_decimal = '.' + usd_mrp_decimal
            usd_mrp_main = int(usd_mrp_float)
            usd_mrp = usd_mrp_main + float(usd_mrp_decimal)
        except Exception as e:
            print('Mrp Exception',e)
            usd_mrp = None


        try:
            usd_price_float = price/exchange_rate
            usd_price_decimal = re.search('[0-9]?[0-9]?\.([0-9][0-9]?)?',str(usd_price_float)).group(1)
            usd_price_decimal = '.' + usd_price_decimal
            usd_price_main = int(usd_price_float)
            usd_price = usd_price_main + float(usd_price_decimal)
        except Exception as e:
            print('Price Exception',e)
            usd_price = None

        try:
            sku_id = base_data['id']
        except:
            sku_id = None

        try:
            long_desc = base_data['longDescription']
            product_description = clean_name(long_desc)
        except Exception as e:
            product_description = ''

        try:
            image_url = 'https://www.dubaidutyfree.com' + base_data['primaryFullImageURL']
        except Exception as e:
            image_url = ''

        try:
            product_url = 'https://www.dubaidutyfree.com' + base_data['route']
        except Exception as e:
            product_url = ''

        try:
            single_discount = base_data['x_simplePromotionDesc']
        except Exception as e:
            single_discount = None

        try:
            multi_buy_discount = base_data['x_multibuyPromotionDesc']
        except Exception as e:
            multi_buy_discount = None

        try:
            specification_string = base_data['x_productSpecification']
            specification_string_clean = clean_name(specification_string)
            specification_string_clean = specification_string_clean.lower()
            specification_json = json.loads(specification_string_clean)

            additional_specs = specification_json.get("more_specifications")
            general_specs = specification_json.get("general")

        except Exception as e:
            specification_json = None

        more_info_list = []
        size = ''

        ## general purposely kept over additional as size is blank in general
        try:
            for key,value in general_specs.items():
                if value:
                    if key.strip() == 'size' and isinstance(value,str):
                        # print("General: ", key, type(value))
                        size = value
                    dict_string = key + ": " + value
                    more_info_list.append(dict_string)
        except:
            pass

        try:
            for key,value in additional_specs.items():
                if value:
                    # print(key,value,type(value))
                    if key.strip() == 'size' and isinstance(value,str):
                        # print("Additional: ",key,value)
                        size = value
                    dict_string = key + ": " + value
                    more_info_list.append(dict_string)
        except Exception as e:
            print(e)
            pass

        more_info_string = ' | '.join(more_info_list)

        if not size:
            size = get_size_from_title(product_name)

        miscellaneous = {
            'multi_discount': multi_buy_discount,
            'days_available': days_available
        }

        miscellaneous_string = json.dumps(miscellaneous)

        dubaiItem['website_id'] = WEBSITE_ID
        dubaiItem['scrape_date'] = SCRAPE_DATE
        dubaiItem['brand'] = brand
        dubaiItem['sku_id'] = sku_id
        dubaiItem['product_name'] = product_name
        dubaiItem['product_url'] = product_url
        dubaiItem['image_url'] = image_url
        dubaiItem['product_description'] = product_description
        dubaiItem['info_table'] = more_info_string
        dubaiItem['price'] = price
        dubaiItem['mrp'] = mrp
        dubaiItem['discount'] = single_discount
        dubaiItem['miscellaneous'] = miscellaneous_string
        dubaiItem['size'] = size
        dubaiItem['high_street_price'] = None
        dubaiItem['usd_price'] = usd_price
        dubaiItem['usd_mrp'] = usd_mrp

        stock_url = f'https://www.dubaidutyfree.com/ccstoreui/v1/stockStatus/{sku_id}?skuId={sku_id}&catalogId='

        yield scrapy.Request(
            url= stock_url, 
            callback=self.parse_product_stock,
            headers=STOCK_HEADERS,
            meta = {'dubaiItem':dubaiItem},
            dont_filter=True
        )

    def parse_product_stock(self, response):

        dubaiItem  = response.meta['dubaiItem']
        json_data = json.loads(response.body)
        qty_left = json_data['inStockQuantity']
        stock_string = json_data['stockStatus'].lower()

        if stock_string == 'in_stock':
            oos = 0
        elif stock_string == 'out_stock':
            oos = 1
        else: ##if it comes here it needs to be fixed
            oos = None

        dubaiItem['out_of_stock'] = oos
        dubaiItem['qty_left'] = qty_left

        yield dubaiItem