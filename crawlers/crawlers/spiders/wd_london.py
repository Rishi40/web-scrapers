from math import prod
from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import CrawlerItem
from worldduty.common_functions import clean_name, get_size_from_title, SCRAPER_URL, BENCHMARK_DATE, SCRAPE_DATE, visited_skus, write_to_log
import datetime
from scrapy import signals

WEBSITE_ID = 1
product_list_limit = 100

CATALOG_HEADERS = {
    'authority': 'london-heathrow.worlddutyfree.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

PRODUCT_HEADERS = {
    'authority': 'london-heathrow.worlddutyfree.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'cookie': 'mage-banners-cache-storage={}; BVBRANDID=ec8710a0-03c9-4a2a-bc14-5819f314d53d; _gcl_au=1.1.177731288.1707832223; ttRV=ret; lynx-cookie=3647230d-b10c-48fd-a25c-5c6f6c42dcc2; wz.uid=Wy035227R014ZjVaSY2y48j27; _fbp=fb.1.1707832224475.1746299972; _hjSessionUser_3369916=eyJpZCI6ImQ1NjhlYTRlLWQ2MjEtNWUxNS05ZGQ3LWZlZjIzOWE4ZTNmMyIsImNyZWF0ZWQiOjE3MDc4MzIyMjcxMjQsImV4aXN0aW5nIjp0cnVlfQ==; _bkrmku=%7B%22user%22%3A%7B%22language%22%3A%22EN%22%2C%22currency%22%3A%22GBP%22%2C%22phone%22%3A%22London%20Heathrow%22%7D%7D; __tmbid=us-1707832227-9e89eab355634629a140f7086ddc6b78; form_key=vbAIk9Te6lKua0vg; AMCVS_B72759175BC87D800A495D6D%40AdobeOrg=1; AMCV_B72759175BC87D800A495D6D%40AdobeOrg=359503849%7CMCIDTS%7C19778%7CMCMID%7C81608700178624963852986820655230303099%7CMCAAMLH-1709421322%7C7%7CMCAAMB-1709421322%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1708823722s%7CNONE%7CvVersion%7C5.0.1; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-messages=; form_key=vbAIk9Te6lKua0vg; PHPSESSID=qa9sq7f24r3i4jsvthh1qklc19; BVBRANDSID=fdf03679-bffa-4666-adb3-4ed11b002d71; _gid=GA1.2.1369653481.1708816526; wz.sid_ed9c5b66-4c16-4f87-b34c-f97a104d31ae=QMZ821o057681791CLN7sm672; wz.flowsMapSegmentKeys=%5B%5D; wz.flowsGroupBySegmentKeys=%5B%5D; _hjSession_3369916=eyJpZCI6IjI4YzZmZDkzLTllMmEtNGE1NS05MDk5LTZjOGMyOGQ5Mzg5NiIsImMiOjE3MDg4MTY1MzEyNzUsInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; Hm_lvt_6e339839fe833a5f48a7fe58a4811e24=1707832228,1708816532; at_check=true; AKA_A2=A; mage-cache-sessid=true; wz.sid=QMZ821o057681791CLN7sm672; BVImplmain_site=11174; _gat_UA-65082267-8=1; RT="z=1&dm=worlddutyfree.com&si=p01fmqf46l&ss=lt0p8gws&sl=1&tt=0&obo=1"; private_content_version=70326cadb4757c93c7fd2806e5b8998b; _ga_S165S0HGB2=GS1.1.1708816525.2.1.1708816821.30.0.0; _ga=GA1.2.1649958422.1707832223; wz.pid=g8Yw81011NP2G68B91B947ix4; _uetsid=977e7780d36a11ee81dfed6d950b80a1; _uetvid=12a154605cd311edbfd4976d3380fc7d; Hm_lpvt_6e339839fe833a5f48a7fe58a4811e24=1708816823; mbox=PC#9119eee6a9c649068f9f9fb5de31b23d.34_0#1772061625|session#3c5e6992eabb402fbafbd5f7d0daec9d#1708818383; section_data_ids={%22messages%22:1708816821%2C%22directory-data%22:1708816821%2C%22customer%22:1708816821%2C%22compare-products%22:1708816821%2C%22last-ordered-items%22:1708816821%2C%22captcha%22:1708816821%2C%22wishlist%22:1708816821%2C%22instant-purchase%22:1708816821%2C%22loggedAsCustomer%22:1708816821%2C%22multiplewishlist%22:1708816821%2C%22persistent%22:1708816821%2C%22review%22:1708816821%2C%22ammessages%22:1708816821%2C%22product_images%22:1708816821%2C%22emporium_customer%22:1708816821%2C%22simpleDutyPaid%22:1708816821%2C%22quota%22:1708816821%2C%22redbydufry%22:1708816821%2C%22banner-data%22:1708816821%2C%22recently_viewed_product%22:1708816821%2C%22recently_compared_product%22:1708816821%2C%22product_data_storage%22:1708816821%2C%22paypal-billing-agreement%22:1708816821%2C%22cart%22:1708816823}; wz.state=1708816827103; wz_visited_pages=%7B%22counter%22:5%7D; _ga_YFVPYK9M08=GS1.2.1708816545.2.0.1708816828.28.0.0',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_wd_london"
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.CrawlerPipeline': 300,
        },
    }

    cookies = {
        'PHPSESSID': '2o3rmj6c2ich332mqqd84b35a2',
        'form_key': 'vbAIk9Te6lKua0vg',
        'terminal_id': '34',
        'store': 'lhr_t5_en'
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category

        url = f'https://london-heathrow.worlddutyfree.com/en/{category}/{sub_category}?p=1&product_list_limit={product_list_limit}'
        manual_benchmark_date = (datetime.date.today().replace(day=1)).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,manual_benchmark_date,sub_category)

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            headers=CATALOG_HEADERS,
            meta = {'visited_sku_list':visited_sku_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://london-heathrow.worlddutyfree.com/en/clinique-superpowder-double-face-makeup-d6b39dbeige-10g'
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
        product_count = response.css('span.toolbar-number::text').get()
        no_of_pages = int(product_count)//product_list_limit + 1
        print("====PRODUCT COUNT====",product_count)
        print("====NUMBER OF PAGES====",no_of_pages)

        # no_of_pages = 1 ## For Testing
        for page_index in range(1,no_of_pages+1):
            url = f'https://london-heathrow.worlddutyfree.com/en/{self.category}/{self.sub_category}?p={page_index}&product_list_limit={product_list_limit}'
            catalogue_links_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=catalogue_links_url, 
                callback=self.parse_catalogue_links, 
                headers=CATALOG_HEADERS,
                meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                dont_filter=True)

    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        page_index = response.meta['page_index']
        product_links = response.css('a.product.photo.product-item-photo::attr(href)').getall()

        for product_link in product_links:
            metadata = {}
            metadata['product_url'] = product_link
            metadata['page_index'] = page_index
            metadata['category'] = self.category
            metadata['sub_category'] = self.sub_category
            final_url = SCRAPER_URL + product_link
            if product_link not in visited_sku_list:
                # print(product_link)
                yield scrapy.Request(
                    url=final_url, 
                    callback=self.parse_product,
                    headers=PRODUCT_HEADERS, 
                    cookies=self.cookies,
                    meta={'metadata':metadata}, 
                    dont_filter=True
                )
            else:
                print("----PRODUCT ALREADY EXISTS----")

    def parse_product(self, response):

        item = CrawlerItem()
        metadata = response.meta['metadata']
        miscellaneous = {} 
        miscellaneous_string = json.dumps(miscellaneous) 

        brand_selectors = [
            'div.logo-text::text',
            'div.logo-text a::text'
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
            product_name = response.css('span.product-name-item::text').get().strip()
        except Exception as e:
            product_name = ''

        try:
            product_url = metadata['product_url']
        except Exception as e:
            product_url = ''

        try:
            image_url = response.css('img#product-image::attr(data-src)').get().strip()
        except Exception as e:
            image_url = ''

        table_data = response.css('table#product-attribute-specs-table tbody tr')
        more_info_list = []

        try:
            for data in table_data:
                key = data.css('th::text').get()
                value = data.css('td::text').get()
                dict_string = key + ": " + value
                more_info_list.append(dict_string)
        except:
            pass

        more_info_string = ' | '.join(more_info_list)

        status_flag = 0

        try:
            price_data_string = response.xpath("//script[contains(.,'swatch-options')]/text()")[1].extract()
            price_data_json = json.loads(price_data_string)
            status_flag = 1
        except:
            price_data_json = ''

        if status_flag:
            base_dict = price_data_json['[data-role=swatch-options]']['Magento_Swatches/js/swatch-renderer']['jsonConfig']
            try:
                options_dict = base_dict['attributes']['421']['options']
            except:
                options_dict = base_dict['attributes']['485']['options']

            if type(options_dict) is not dict: # Edge case handling
                options_dict = options_dict[0]
                size_option_key = options_dict['products'][0]
                size = options_dict['label']
                oos = options_dict['out_of_stock']
                mrp = base_dict['promotionsData'][size_option_key]['productUtils']['regularPrice']
                price = base_dict['high_street_prices'][size_option_key]['price']
                discount = base_dict['promotionsData'][size_option_key]['productUtils']['discount']
                high_street_price = base_dict['high_street_prices'][size_option_key]['high_street_price']
                qty_left = base_dict['optionStock'][size_option_key]
                sku_id = base_dict['sap_codes'][size_option_key]

                product_description = base_dict['descriptions'][size_option_key]

                item['website_id'] = WEBSITE_ID
                item['scrape_date'] = SCRAPE_DATE
                item['category'] = metadata['category']
                item['sub_category'] = metadata['sub_category']
                item['brand'] = brand
                item['sku_id'] = sku_id
                item['product_name'] = product_name
                item['product_url'] = product_url
                item['image_url'] = image_url
                item['product_description'] = product_description
                item['info_table'] = more_info_string
                item['out_of_stock'] = 1 if oos else 0
                item['price'] = price
                item['mrp'] = mrp
                item['high_street_price'] = high_street_price
                item['discount'] = discount
                item['size'] = size
                item['qty_left'] = qty_left
                item['usd_price'] = None
                item['usd_mrp'] = None
                item['miscellaneous'] = miscellaneous_string

                yield item

            else:
                for key,value in options_dict.items():
                    size_option_key = value['products'][0]
                    size = value['label']
                    oos = value['out_of_stock']
                    mrp = base_dict['promotionsData'][size_option_key]['productUtils']['regularPrice']
                    price = base_dict['high_street_prices'][size_option_key]['price']
                    discount = base_dict['promotionsData'][size_option_key]['productUtils']['discount']
                    high_street_price = base_dict['high_street_prices'][size_option_key]['high_street_price']
                    qty_left = base_dict['optionStock'][size_option_key]
                    sku_id = base_dict['sap_codes'][size_option_key]

                    product_description = base_dict['descriptions'][size_option_key]

                    print("IN IF",product_url)

                    item['website_id'] = WEBSITE_ID
                    item['scrape_date'] = SCRAPE_DATE
                    item['category'] = metadata['category']
                    item['sub_category'] = metadata['sub_category']
                    item['brand'] = brand
                    item['sku_id'] = sku_id
                    item['product_name'] = product_name
                    item['product_url'] = product_url
                    item['image_url'] = image_url
                    item['product_description'] = product_description
                    item['info_table'] = more_info_string
                    item['out_of_stock'] = 1 if oos else 0
                    item['price'] = price
                    item['mrp'] = mrp
                    item['high_street_price'] = high_street_price
                    item['discount'] = discount
                    item['size'] = size
                    item['qty_left'] = qty_left
                    item['usd_price'] = None
                    item['usd_mrp'] = None
                    item['miscellaneous'] = miscellaneous_string

                    yield item

        else:
            size = get_size_from_title(product_name)

            try:
                product_description = response.css('div.product.attribute.description div.value::text').get()
            except Exception as e:
                product_description = ''

            try:
                oos_selector_1 = response.css('button#product-addtocart-button span::text').get()
                oos_selector_2 = response.css('span#product-addtocart-button span::text').get()

                if oos_selector_1:
                    oos = 0
                elif oos_selector_2:
                    oos = 1
                else:
                    ##If here then have to fix error
                    oos = None
                
            except Exception as e:
                oos = None

            try:
                mrp_selector = response.css('span[data-price-type="oldPrice"]')
                mrp = mrp_selector.css('span.price::text').get().replace('£','')
            except:
                mrp = None

            try:
                price_selector = response.css('span[data-price-type="finalPrice"]')
                price = price_selector.css('span.price::text').get().replace('£','')
            except:
                price = None

            try:
                discount_selector = response.xpath("//script[contains(.,'flag-discount')]/text()")[0].extract()
                discount_selector_json = json.loads(discount_selector)
                discount_selector_string = discount_selector_json["*"]["Magento_Ui/js/core/app"]["components"]["productRoundels"]["configuration"]["roundel"]
                ds = Selector(text=discount_selector_string)
                discount = ds.css('div.flag-discount::text').get().replace('%','')
                discount = int(discount)
            except Exception as e:
                discount = None

            try:
                high_street_price = response.css('span.hsp_price::text').get().replace('£','')
            except:
                high_street_price = None

            try:
                qty_selector_string = response.xpath("//script[contains(.,'in_stock')]/text()")[0].extract()
                qty_selector_json = json.loads(qty_selector_string)
                qty_left = qty_selector_json['*']['Magento_Ui/js/core/app']['components']['stockFlag']['configuration']['qty']
            except:
                qty_left = None

            try:
                sku_id = response.css('div[itemprop="sku"]::text').get()
            except:
                sku_id = None

            print("IN ELSE",product_url)

            item['website_id'] = WEBSITE_ID
            item['scrape_date'] = SCRAPE_DATE
            item['category'] = metadata['category']
            item['sub_category'] = metadata['sub_category']
            item['brand'] = brand
            item['sku_id'] = sku_id
            item['product_name'] = product_name
            item['product_url'] = product_url
            item['image_url'] = image_url
            item['product_description'] = product_description
            item['info_table'] = more_info_string
            item['out_of_stock'] = 1 if oos else 0
            item['price'] = price
            item['mrp'] = mrp
            item['high_street_price'] = high_street_price
            item['discount'] = discount
            item['size'] = size
            item['qty_left'] = qty_left
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = miscellaneous_string

            yield item