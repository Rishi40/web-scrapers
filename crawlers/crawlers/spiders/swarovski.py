import scrapy
import json
from worldduty.items import FactiveItem
from worldduty.common_functions import get_size_from_title, SCRAPER_URL, visited_sku_ids, BENCHMARK_DATE
from datetime import datetime

WEBSITE_ID = 55
PRODUCT_LIST_LIMIT = 100

CATALOG_HEADERS = {
    'authority': 'www.swarovski.ae',
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
    name = "scrape_swarovski"
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
        visited_skus_list = visited_sku_ids(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        url = f'https://www.swarovski.ae/on/demandware.store/Sites-Swarovski_AE-Site/en_AE/Search-UpdateGrid?cgid=collection-jewellery&prefn1=displayInPLP&prefv1=true&start=0&sz={PRODUCT_LIST_LIMIT}'

        catalogue_url = SCRAPER_URL + url
        yield scrapy.Request(
            url=catalogue_url, 
            headers=CATALOG_HEADERS,
            callback=self.parse_catalogue_pages,
            meta={'visited_skus_list':visited_skus_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.swarovski.ae/twist-wrap-ring-white-rhodium-plated/M5580952.html?dwvar_M5580952_size=50'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['product_url'] = url
        # yield scrapy.Request(
        #     url=final_url, 
        #     callback=self.parse_product,
        #     meta={'metadata':metadata}
        # )

    def parse_catalogue_pages(self, response):
        visited_skus_list = response.meta['visited_skus_list']

        try:
            product_count = response.css('progress.show-more-progress::attr(max)').get() 
            product_count = int(product_count)
        except:
            product_count = 0
        
        no_of_pages = product_count//PRODUCT_LIST_LIMIT + 1
        offset = 0

        print("====PRODUCT COUNT====",product_count)
        print("====no_of_pages====",no_of_pages)

        # no_of_pages = 3 ## For Testing
        for page_index in range(no_of_pages):
            url = f'https://www.swarovski.ae/on/demandware.store/Sites-Swarovski_AE-Site/en_AE/Search-UpdateGrid?cgid=collection-jewellery&prefn1=displayInPLP&prefv1=true&start={offset}&sz={PRODUCT_LIST_LIMIT}'
            catalogue_links_url = SCRAPER_URL + url

            yield scrapy.Request(
                url=catalogue_links_url,
                headers=CATALOG_HEADERS, 
                callback=self.parse_catalogue_links, 
                meta={'page_index':page_index,'visited_skus_list':visited_skus_list}, 
                dont_filter=True)
            
            offset += PRODUCT_LIST_LIMIT

    def parse_catalogue_links(self, response):
        product_cards = response.css('div.js-product-tile-wrapper.product-tile-wrapper-new')
        visited_skus_list = response.meta['visited_skus_list']

        if product_cards:
            for product in product_cards:
                sku_id = product.css('a.product-tile::attr(data-pid)').get() 
                product_link_end = product.css('a.product-tile::attr(href)').get() 
                product_link = "https://www.swarovski.ae" + product_link_end
                image_url = product.css('img.tile-image::attr(data-src)').get()

                metadata = {}
                metadata['product_url'] = product_link
                metadata['image_url'] = image_url
                final_url = SCRAPER_URL + product_link

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
        metadata = response.meta['metadata']
        miscellaneous = {}
        scrape_date = datetime.today().strftime('%Y-%m-%d')

        try:
            string_data = response.css('script[type="application/ld+json"]::text').get().strip()
            json_data = json.loads(string_data)
        except:
            json_data = {}

        if json_data:
            try:
                brand = json_data.get('brand')
            except Exception as e:
                brand = None

            try:
                sku_id = json_data.get('sku')
            except:
                sku_id = None

            try:
                product_name = json_data.get('name')
            except:
                product_name = None

            try:
                product_description = json_data.get('description')
            except:
                product_description = None

            try:
                oos_string = json_data.get('offers').get('availability').lower()
                if 'instock' in oos_string:
                    oos = 0
                else:
                    oos = 1
            except Exception as e:
                oos = None

            try:
                mrp = response.css('span.strike-through span::attr(content)').get() 
            except:
                mrp = None

            try:
                price = json_data.get('offers').get('price')
            except:
                price = None

            try:
                discount = response.css('span.discount-badge::text').get().replace('−','')
            except:
                discount = None

            more_info_list = []
            table_data = response.css('ul.product-additional-info li')

            try:
                for data in table_data:
                    info = data.css('::text').get()
                    more_info_list.append(info)
            except:
                more_info_list = []

            more_info_string = ' | '.join(more_info_list)

            try:
                size_list = response.css('div.buttons-size button::attr(data-attr-value)').getall()
                size = ' | '.join(size_list)
            except:
                size = None

            miscellaneous_string = json.dumps(miscellaneous)

            item['website_id'] = WEBSITE_ID
            item['scrape_date'] = scrape_date
            item['category'] = self.category
            item['sub_category'] = self.sub_category
            item['brand'] = brand
            item['sku_id'] = sku_id
            item['product_name'] = product_name
            item['product_url'] = metadata['product_url']
            item['image_url'] = metadata['image_url']
            item['product_description'] = product_description
            item['info_table'] = more_info_string
            item['out_of_stock'] = oos
            item['price'] = price
            item['mrp'] = mrp
            item['high_street_price'] = None
            item['discount'] = discount
            item['size'] = size
            item['qty_left'] = None
            item['usd_price'] = None
            item['usd_mrp'] = None
            item['miscellaneous'] = None

            yield item