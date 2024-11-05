from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_name, get_size_from_title, SCRAPER_URL, write_to_log, visited_sku_ids, BENCHMARK_DATE
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 14
PRODUCT_LIST_LIMIT = 20

sub_category_id_map = {
    'fragrances': '53',
    'make-up': '18',
    'skin-care': '17',
    'wines': '67',
    'spirits': '78'
}

CATALOG_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'app-domain': 'www.dfs.com',
    'app-key': 'c53fae5119f0472f90040e1cf3e35a27',
    'app-platform': 'web',
    'app-version': '8.6.3',
    'channelid': '3',
    'division': 'new-york',
    'duty-method': 'DutyFree',
    'flight-type': 'international',
    'loyaltsession': '',
    'onlinestorecode': '1224',
    'origin': 'https://www.dfs.com',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'subsiteid': '4002',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'usersession': 'b05257eb-ea2e-4967-a406-b1b44c7e8f85',
    'Cookie': 'ak_bmsc=6AEF5C84479794324C7E77F35951188B~000000000000000000000000000000~YAAQHGfRF4DeMwiQAQAAmJKkERha72bYu/9ZMsp+GMSVl1dG03Q6c4FAah1F1rBKPwUDbUKCQdO0O/FPm/pCtFjWM8Uq5xpZbYGhiMWUmUqIVvL54fG8eBjduwh8ou3DseQkaaKue60IAKT9ZuLpLCRntfXkg4Ftn65reBq6Q1D3Ia+9PZEsXtBdSIiItRM32ML8oMp4/cxTrB+xE50xr06BHo6eaMh/xk1Rrwn1NLkRfFgVFlNu1YU4J3Lyvj6lARKv9wkmYnxKbHayBAqt9dq2HbvsARXaus791Imm3yc//EBznSlb53n1glYO58w8tKH+CeEFkneV2zQQys2RIgtUBoq+yzIaCWjhAhd0sPmrUQ6EbiBhbw==; bm_sv=A15B5DC13C800F8BE799F7BE162D848D~YAAQHGfRF5UyNAiQAQAAtuulERh892J7P7hoyCelfyS13zXZTdZ5orW6ya414S1X8XA1FwIT2W/FgffbIFZiHZwv4zqFOLBMMn/6x2StiEFX9PV/m2bkHA0BBJsCJshrkfw6A3oRlvh2mzYTrtO2CxOfYAJ+yCMkPFct0CHZuSxG/Ya4yZG6XdcuvAn8W8w9FG6cuD4TBvDW6pdHR5KXq+cwCQ0lfPXRb/Fuumrlaa6hpWReb7U/jAzwOul9~1; dtCookie=v_4_srv_1_sn_2F45927EC915C54AF2516C23248D0688_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1_rcs-3Acss_0'
}

PRODUCT_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'app-domain': 'www.dfs.com',
    'app-key': 'c53fae5119f0472f90040e1cf3e35a27',
    'app-platform': 'web',
    'app-version': '8.6.3',
    'channelid': '3',
    'division': 'new-york',
    'duty-method': 'DutyFree',
    'flight-type': 'international',
    'loyaltsession': '',
    'onlinestorecode': '1224',
    'origin': 'https://www.dfs.com',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'subsiteid': '4002',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'usersession': 'b05257eb-ea2e-4967-a406-b1b44c7e8f85',
    'Cookie': 'ak_bmsc=6AEF5C84479794324C7E77F35951188B~000000000000000000000000000000~YAAQHGfRF4DeMwiQAQAAmJKkERha72bYu/9ZMsp+GMSVl1dG03Q6c4FAah1F1rBKPwUDbUKCQdO0O/FPm/pCtFjWM8Uq5xpZbYGhiMWUmUqIVvL54fG8eBjduwh8ou3DseQkaaKue60IAKT9ZuLpLCRntfXkg4Ftn65reBq6Q1D3Ia+9PZEsXtBdSIiItRM32ML8oMp4/cxTrB+xE50xr06BHo6eaMh/xk1Rrwn1NLkRfFgVFlNu1YU4J3Lyvj6lARKv9wkmYnxKbHayBAqt9dq2HbvsARXaus791Imm3yc//EBznSlb53n1glYO58w8tKH+CeEFkneV2zQQys2RIgtUBoq+yzIaCWjhAhd0sPmrUQ6EbiBhbw==; bm_sv=A15B5DC13C800F8BE799F7BE162D848D~YAAQUYIsMUv14wyQAQAA9BC1ERjCI0O8lY6rbhEFMd2Zfx30PosFF0dkd3LKyXJYXqWzp3z23odubxiqkd/ok6JDjhihKnYzYkbOt0kW9B8ZI9lL2SbxbN/8MmBG5tOfUB++jl6AtGAiSge7K9IOgIUDDGdpdX5xKRXfmUwRPlhw3L/4P73cRasDfxJP/lrekztBe63EhyyiN8jtZ3wSh5Isp/wAULMC/XjXmLDubAndJsD8FFXDEID5Su72~1; dtCookie=v_4_srv_1_sn_2F45927EC915C54AF2516C23248D0688_perc_100000_ol_0_mul_1_app-3Aea7c4b59f27d43eb_1_rcs-3Acss_0'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_dfa"
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

        url = f"https://mulapi.dfs.com/prod/digital-neo-commonsvc/v1/neo/mobile_api/api/v1/rollout-mp-service/product/list/category/data?page=1&pageSize=20&title={sub_category_id}"

        yield scrapy.Request(
            url=url, 
            headers=CATALOG_HEADERS,
            callback=self.parse_catalogue_pages,
            meta={
                'visited_sku_list': visited_sku_list,
                'sub_category': sub_category,
                'sub_category_id': sub_category_id
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://jfk.dutyfreeamericas.com/kiehls-facial-fuel-energizing-tonic-for-men-250ml.html'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['image_url'] = 'test.com'
        # metadata['discount'] = '32%'
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
        sub_category = response.meta['sub_category']
        sub_category_id = response.meta['sub_category_id']

        try:
            catalogue_data_string = response.body
            catalogue_data_json = json.loads(catalogue_data_string)
        except:
            catalogue_data_json = {}

        if catalogue_data_json:
            product_count = catalogue_data_json.get('total')
            no_of_pages = int(product_count)//PRODUCT_LIST_LIMIT + 1
            print("====PRODUCT COUNT====",product_count)
            print("Total Pages ---->",no_of_pages)

            # no_of_pages = 2
            for page_index in range(1,no_of_pages+1):
                url = f"https://mulapi.dfs.com/prod/digital-neo-commonsvc/v1/neo/mobile_api/api/v1/rollout-mp-service/product/list/category/data?page={page_index}&pageSize=20&title={sub_category_id}"

                yield scrapy.Request(
                    url=url, 
                    headers=CATALOG_HEADERS,
                    callback=self.parse_catalogue_links,
                    meta={
                        'visited_sku_list': visited_sku_list,
                        'sub_category': sub_category,
                        'sub_category_id': sub_category_id
                    },
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']

        try:
            page_data_string = response.body
            page_data_json = json.loads(page_data_string)
        except:
            page_data_json = {}

        if page_data_json:
            products = page_data_json.get('items')
            for product in products:
                sku_id = product.get('csku')

                if sku_id not in visited_sku_list:
                    product_api_url = f"https://mulapi.dfs.com/prod/digital-neo-commonsvc/v1/neo/mobile_api/api/v1/rollout-mp-service/product/goods/detail/{sku_id}"
                    yield scrapy.Request(
                        url=product_api_url, 
                        headers=PRODUCT_HEADERS,
                        callback=self.parse_product_variations, 
                        dont_filter=True
                    )
                else:
                    print("-----PRODUCT EXISTS-----")
        
        else:
            print("------NO PRODUCTS EXIST------")

    def parse_product_variations(self, response):

        try:
            string_response = response.body
            json_response = json.loads(string_response)
            variants = json_response.get('specs')
        except:
            variants = {}

        for variant in variants:
            sku_id = variant.get('csku')
            product_api_url = f"https://mulapi.dfs.com/prod/digital-neo-commonsvc/v1/neo/mobile_api/api/v1/rollout-mp-service/product/goods/detail/{sku_id}"
            yield scrapy.Request(
                url=product_api_url, 
                headers=PRODUCT_HEADERS,
                callback=self.parse_product_variations_api, 
                dont_filter=True
            )

    def parse_product_variations_api(self, response):

        item = FactiveItem()
        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            variant_string_response = response.body
            variant_json_response = json.loads(variant_string_response)
        except:
            variant_json_response = {}

        if variant_json_response:
            try:
                sku_id = variant_json_response.get('csku')
            except Exception as e:
                sku_id = None

            try:
                brand = variant_json_response.get('brandName')
            except Exception as e:
                brand = None

            try:
                product_name = variant_json_response.get('name')
            except Exception as e:
                product_name = None

            try:
                category_name_en = variant_json_response.get('categoryNameEn')
                category_name_en = category_name_en.lower().replace(' ','-')
            except Exception as e:
                category_name_en = None

            try:
                brand_en = variant_json_response.get('brandNameEn')
                brand_en = brand_en.lower().replace(' ','-')
            except Exception as e:
                brand_en = None

            try:
                product_name_en = variant_json_response.get('nameEn')
                product_name_en = product_name_en.lower().replace(' ','-')
            except Exception as e:
                product_name_en = None

            try:
                product_url = f"https://www.dfs.com/en/new-york/eshop/products/{brand_en}/{category_name_en}/{product_name_en}/{sku_id}"
            except Exception as e:
                product_url = None

            try:
                image_url = variant_json_response.get('mainPics')[0]
            except Exception as e:
                image_url = None

            try:
                size = variant_json_response.get('specName')
            except Exception as e:
                size = None

            try:
                product_description = variant_json_response.get('content')
            except Exception as e:
                product_description = None

            try:
                oos_string = variant_json_response.get('soldOut')
                if not oos_string:
                    oos = 0
                else:
                    oos = 1
            except Exception as e:
                oos = None

            try:
                price = variant_json_response.get('salePrice').get('amount')
            except:
                price = None

            try:
                mrp = variant_json_response.get('linePrice').get('amount')
            except:
                mrp = None

            try:
                qty_left = variant_json_response.get('availableStock')
            except:
                qty_left = None

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
            item['miscellaneous'] = None

            yield item

'''
product_url
'''