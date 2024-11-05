import scrapy
import json
import requests
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_product_description, get_size_from_title, SCRAPER_URL, visited_sku_ids, BENCHMARK_DATE, write_to_log
from datetime import datetime
import math
from scrapy import signals
from scrapy.selector import Selector

WEBSITE_ID = 52
PRODUCT_LIST_LIMIT = 144

sub_category_id_map = {
    'fragrance': ['beauty-fragrance','beauty-fragrance'],
    'makeup': ['beauty-makeup','beauty-makeup'],
    'skincare': ['beauty-skincare','beauty-skincare'],
    'men-sunglasses': ['mens-accessories-sunglasses','men-mens-accessories-sunglasses'],
    'women-sunglasses': ['womens-accessories-sunglasses','women-womens-accessories-sunglasses'],
    'jewelry': ['jewelry','jewelry']
}

CATALOGUE_HEADERS = {
    'authority': 'bloomingdales.ae',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_bloomingdales"
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.FactivePipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category
        sub_category_endpoint = sub_category_id_map.get(sub_category)[0]
        sub_category_id = sub_category_id_map.get(sub_category)[1]
        visited_skus_list = visited_sku_ids(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = f'https://bloomingdales.ae/{sub_category_endpoint}/'
        catalogue_url = SCRAPER_URL + url

        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            meta={
                'sub_category_id':sub_category_id, 
                'visited_skus_list':visited_skus_list
            },
            dont_filter=True
        )

        # For Testing

        # url = 'https://bloomingdales.ae/armani-si-intense-eau-de-parfum-BEA216646003.html'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url

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

        sub_category_id = response.meta['sub_category_id']
        visited_skus_list = response.meta['visited_skus_list']

        try:
            product_count = response.css('div.b-progress-bar::attr(aria-valuemax)').get() 
            product_count = int(float(product_count))
        except:
            product_count = 0
        
        no_of_pages = product_count//PRODUCT_LIST_LIMIT + 1
        offset = 0

        print("====PRODUCT COUNT====",product_count)
        print("====no_of_pages====",no_of_pages)
        # no_of_pages = 1 ## For Testing
        for page_index in range(no_of_pages):
            url = f"https://bloomingdales.ae/on/demandware.store/Sites-BloomingDales_AE-Site/en_AE/Search-UpdateGrid?cgid={sub_category_id}&pmin=6%2e00&start={offset}&sz={PRODUCT_LIST_LIMIT}&icgid={sub_category_id}"

            yield scrapy.Request(
                url=url, 
                callback=self.parse_catalogue_links, 
                headers=CATALOGUE_HEADERS,
                meta={
                    'page_index':page_index,
                    'visited_skus_list':visited_skus_list
                }, 
                dont_filter=True)

            offset = offset + PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):

        product_cards = response.css('div.js-product-tile')
        visited_skus_list = response.meta['visited_skus_list']

        if product_cards:
            for product in product_cards:
                sku_id = product.css('::attr(data-pid)').get() 
                product_link_end = product.css('div.pdp-link a.js-product-name::attr(href)').get() 
                product_link = "https://bloomingdales.ae" + product_link_end

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category

                final_url = SCRAPER_URL + product_link
                # print("------------->", product_link)
                if sku_id.strip() not in visited_skus_list:
                    yield scrapy.Request(
                        url=final_url, 
                        callback=self.parse_product, 
                        meta={'metadata':metadata}, 
                        dont_filter=True
                    )
                else:
                    print("-------- PRODUCT ALREADY EXISTS --------")

        else:
            print("----NO PRODUCTS FOUND----")

    def parse_product(self, response):

        item = FactiveItem()
        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')
        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        variation_container = response.css('div.blm-pdpmain__info')
        size_variation_list = variation_container.css('div.js-size-item-container button.blm-attribute__button::attr(data-url)').getall()
        color_variation_list = variation_container.css('div.blm-producttile__swatches-list a.blm-producttile__swatches-item::attr(data-url)').getall()
        bundled_variation_list = variation_container.css('div.blm-pdp__bundle-items div.blm-pdp__bundle-item')

        if size_variation_list and not color_variation_list and not bundled_variation_list:
            for size_variation in size_variation_list:
                try:
                    string_data = requests.request("GET", size_variation)
                    json_data = json.loads(string_data.text)
                except Exception as e:
                    json_data = {}

                if json_data:
                    try:
                        brand = json_data.get('product').get('brand')
                    except Exception as e:
                        brand = None

                    try:
                        product_name = json_data.get('product').get('productName')
                    except Exception as e:
                        product_name = None

                    try:
                        sku_id = json_data.get('product').get('id')
                    except:
                        sku_id = None

                    try:
                        image_url = json_data.get('product').get('images').get('large')[0].get('url')
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

                    try:
                        size = json_data.get('product').get('size')
                    except:
                        size = None

                    try:
                        qty_left = json_data.get('product').get('availability').get('availableQuantity')
                    except:
                        qty_left = None

                    try:
                        master_product_id = json_data.get('product').get('masterProductId')
                        if master_product_id:
                            miscellaneous['master_product_id'] = master_product_id
                    except:
                        master_product_id = None

                    try:
                        rating = json_data.get('product').get('rating')
                        if rating:
                            miscellaneous['rating'] = rating
                    except:
                        rating = None

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
                    item['product_description'] = None
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

        elif color_variation_list and not (size_variation_list and len(size_variation_list) > 1) and not bundled_variation_list:
            for color_variation in color_variation_list:
                try:
                    string_data = requests.request("GET", color_variation)
                    json_data = json.loads(string_data.text)
                except Exception as e:
                    json_data = {}

                if json_data:
                    try:
                        brand = json_data.get('product').get('brand')
                    except Exception as e:
                        brand = None

                    try:
                        product_name = json_data.get('product').get('productName')
                    except Exception as e:
                        product_name = None

                    try:
                        sku_id = json_data.get('product').get('id')
                    except:
                        sku_id = None

                    try:
                        image_url = json_data.get('product').get('images').get('large')[0].get('url')
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

                    try:
                        size = json_data.get('product').get('size')
                    except:
                        size = None

                    try:
                        color_code = json_data.get('product').get('variationAttributes')[0].get('selectedValue')
                        if color_code:
                            miscellaneous['color_code'] = color_code
                    except:
                        color_code = None

                    try:
                        qty_left = json_data.get('product').get('availability').get('availableQuantity')
                    except:
                        qty_left = None

                    try:
                        master_product_id = json_data.get('product').get('masterProductId')
                        if master_product_id:
                            miscellaneous['master_product_id'] = master_product_id
                    except:
                        master_product_id = None

                    try:
                        rating = json_data.get('product').get('rating')
                        if rating:
                            miscellaneous['rating'] = rating
                    except:
                        rating = None

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
                    item['product_description'] = None
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

        else:
            try:
                string_data = response.css('script[type="application/ld+json"]::text').get()
                json_data = json.loads(string_data.strip())
            except Exception as e:
                json_data = {}

            if json_data:
                product_container = response.css('div.js-main-product')

                try:
                    brand = json_data.get('brand').get('name') 
                except Exception as e:
                    brand = None

                try:
                    product_name = json_data.get('name')
                except Exception as e:
                    product_name = None

                try:
                    sku_id = json_data.get('sku')
                except:
                    sku_id = None

                try:
                    image_url = json_data.get('image')[0]
                except:
                    image_url = None

                try:
                    price = json_data.get('offers').get('price')
                except:
                    price = None

                try:
                    mrp = product_container.css('span.blm-price__standard span.blm-price__value::attr(content)').get()
                except:
                    mrp = None

                try:
                    oos_string = json_data.get('offers').get('availability')
                    if 'instock' in oos_string.lower():
                        oos = 0
                    else:
                        oos = 1
                except:
                    oos = None

                try:
                    size = product_container.css('span.js-size-label::text').get()
                except:
                    size = None

                try:
                    color = product_container.css('span.js-color-label-value::text').getall()
                    if len(color) == 1:
                        miscellaneous['color_code'] = color[0]
                except:
                    color = None

                try:
                    master_product_id = product_container.css('::attr(data-listpid)').get()
                    if master_product_id:
                        miscellaneous['master_product_id'] = master_product_id
                except:
                    master_product_id = None

                try:
                    bundledItems = product_container.css('div.blm-pdp__bundle-items div.blm-pdp__bundle-item')
                    bundledItemsList = []
                    for bundledItem in bundledItems:
                        bundledItemUrlEndPoint = bundledItem.css('img.blm-pdp__bundle-image::attr(data-url)').get()
                        bundledItemUrl = 'https://bloomingdales.ae' + bundledItemUrlEndPoint
                        try:
                            bundledItemData = requests.request("GET", bundledItemUrl)
                            bundledItemResponse = Selector(text=bundledItemData.text)
                        except Exception as e:
                            bundledItemResponse = None

                        if bundledItemResponse:
                            bItem = {}
                            try:
                                bsku_id = bundledItem.css('::attr(data-pid)').get()
                                bItem['sku_id'] = bsku_id
                            except:
                                bsku_id = None

                            try:
                                bmastersku_id = bundledItemResponse.css('div.js-product-detail::attr(data-listpid)').get()
                                bItem['mastersku_id'] = bmastersku_id
                            except:
                                bmastersku_id = None

                            try:
                                bproduct_url_endpoint = bundledItemResponse.css('a.js-quickview-pdp-link::attr(href)').get()
                                bproduct_url = 'https://bloomingdales.ae' + bproduct_url_endpoint
                                bItem['product_url'] = bproduct_url
                            except:
                                bproduct_url = None

                            try:
                                bbrand = bundledItem.css('span.blm-pdp__bundle-brand::text').get()
                                bItem['brand'] = bbrand
                            except:
                                bbrand = None

                            try:
                                bproduct_name = bundledItem.css('div.blm-pdp__bundle-name::text').get()
                                bItem['product_name'] = bproduct_name
                            except:
                                bproduct_name = None

                            try:
                                bprice = bundledItemResponse.css('span.blm-price__sale span.blm-price__value::attr(content)').get()
                                bItem['price'] = bprice
                            except:
                                bprice = None

                            try:
                                bsize = bundledItem.css('div.js-size-item-container::attr(data-current-item-size-id)').get()
                                bItem['size'] = bsize
                            except:
                                bsize = None

                            try:
                                bcolor = bundledItem.css('div.js-color-item-container::attr(data-current-item-color-id)').get()
                                bItem['color'] = bcolor
                            except:
                                bcolor = None

                            bundledItemsList.append(bItem) 

                    if bundledItemsList:
                        miscellaneous['bundledItems'] = bundledItemsList

                except Exception as e:
                    pass

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
                item['product_description'] = None
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