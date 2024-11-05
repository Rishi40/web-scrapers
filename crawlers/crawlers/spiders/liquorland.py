from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_name, get_size_from_title, SCRAPER_URL, visited_skus, get_web_page, BENCHMARK_DATE
from datetime import datetime
import time

WEBSITE_ID = 27
PRODUCT_LIST_LIMIT = 20
STORE_ID = 1828

sub_category_id_map = {
    'spirits': 1495,
    'liqueurs': 1496,
    'beer': 1507,
    'craft-beer': 1702,
    'wine': 1497,
    'cider': 1701
}

HEADERS = {
    'authority': 'www.liquorland.co.nz',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'cookie': 'lq-store-selection=1828;lq-age-gate=true',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}

COOKIES = {'lq-age-gate': 'true','lq-store-selection':'1828'}

def get_product_description(response):
    product_description_raw_list = []
    values_list = []

    main_description_list = [
        'div.m-b-1::text',
        'div.m-b-1 div::text',
        'div.m-b-1 p::text',
        'div.m-b-1 span::text',
        'div.m-b-1 p em::text'
    ]

    for md in main_description_list:
        desc_list = response.css(md).getall()
        desc_list_clean = [desc.strip() for desc in desc_list if desc.strip()] 
        product_description_raw_list.append(desc_list_clean)

    product_description_list = [item for pd in product_description_raw_list for item in pd]
    attribute_list = response.css('div.m-b-1 strong::text').getall()

    if attribute_list:
        for pd in product_description_list:
            try:
                if pd.strip().startswith(':'):
                    values_list.append(pd)
            except:
                pass

    for a,b in zip(attribute_list,values_list):
        try:
            product_description_list.remove(b)
        except:
            pass
        dict_string = a + b
        product_description_list.append(dict_string)

    product_description = '\n'.join(product_description_list)
    return product_description


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
    name = "scrape_liquorland"
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 5,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.FactivePipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category
        sub_category_id = sub_category_id_map[sub_category]

        second_day_of_current_month = datetime.today().replace(day=2).strftime("%Y-%m-%d")
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        web_pages_list = collect_web_pages(sub_category)

        url = f'https://www.liquorland.co.nz/shop/{sub_category}?FF=&InStoreOnly=false&P.StartPage=1&P.LoadToPage=1&CategoryId={sub_category_id}&sorting=Suggested&SelectedView=0'
        # catalogue_url = SCRAPER_URL + url + '&keep_headers=true'
        catalogue_url = url

        yield scrapy.Request(
            url=catalogue_url, 
            callback=self.parse_catalogue_pages,
            cookies=COOKIES,
            meta = {
                'sub_category_id':sub_category_id,
                'visited_sku_list':visited_sku_list,
                'web_pages_list': web_pages_list,
            },
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.liquorland.co.nz/benedictine-700ml'
        # metadata = {}
        # final_url = SCRAPER_URL + url + '&keep_headers=true'
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # metadata['oos'] = 0
        # metadata['mrp'] = 56.66
        # metadata['price'] = 50.00

        # yield scrapy.Request(
        #     url=final_url, 
        #     cookies=COOKIES,
        #     callback=self.parse_product,
        #     meta={'metadata':metadata}
        # )

    def parse_catalogue_pages(self, response):
        sub_category_id = response.meta['sub_category_id']
        visited_sku_list = response.meta['visited_sku_list']
        web_pages_list = response.meta['web_pages_list']

        for web_pages in web_pages_list:
            offset_start = web_pages[1]
            offset_end = web_pages[2]

            print(offset_start,offset_end)
            url = f'https://www.liquorland.co.nz/shop/{self.sub_category}?FF=&InStoreOnly=false&P.StartPage={offset_start}&P.LoadToPage={offset_end}&CategoryId={sub_category_id}&sorting=Suggested&SelectedView=0'
            # catalogue_links_url = SCRAPER_URL + url + '&keep_headers=true'
            catalogue_links_url = url

            print("------------------>",catalogue_links_url)

            yield scrapy.Request(
                url=catalogue_links_url, 
                callback=self.parse_catalogue_links,
                cookies=COOKIES,
                meta = {'sub_category_id':sub_category_id, 'visited_sku_list': visited_sku_list},
                dont_filter=True
            )
                
    def parse_catalogue_links(self, response):
        visited_sku_list = response.meta['visited_sku_list']
        product_cards = response.css('div#productListView div[itemprop="itemListElement"]') 

        if product_cards:
            for product in product_cards:

                try:
                    product_end_point = product.css('a.d-block.product-detail::attr(href)').get() 
                    product_link = 'https://www.liquorland.co.nz' + product_end_point
                except:
                    product_link = None
                
                try:
                    mrp = product.css('p.list-price span.was-price::text').get().replace('$','')
                except:
                    mrp = None

                try:
                    price = product.css('p.list-price span.now-price::text').get().replace('$','')
                except:
                    price = None

                if not price:
                    try:
                        price = product.css('p.list-price span.current-price::text').get().replace('$','')
                    except:
                        price = None

                try:
                    oos_section = product.css('div.bg-lightest').get()
                    if oos_section:
                        oos = 1
                    else:
                        oos = 0
                except:
                    oos = None

                metadata = {}
                metadata['product_url'] = product_link
                metadata['category'] = self.category
                metadata['sub_category'] = self.sub_category
                metadata['mrp'] = mrp
                metadata['price'] = price
                metadata['oos'] = oos

                final_url = SCRAPER_URL + product_link + '&keep_headers=true'

                if product_link not in visited_sku_list:
                    # print(metadata)
                    yield scrapy.Request(
                        url=final_url, 
                        cookies=COOKIES,
                        callback=self.parse_product,
                        meta={'metadata':metadata}
                    )

            print("TOTAL PRODUCTS------>", len(product_cards))
        else:
            print("NO PRODUCTS FOUND")

    def parse_product(self, response):

        item = FactiveItem()
        scrape_date = datetime.today().strftime('%Y-%m-%d')
        metadata = response.meta['metadata']

        mrp = metadata['mrp']
        price = metadata['price']
        oos = metadata['oos']

        try:
            sku_id = response.css('p.paragraph-sm.m-b-1::text').get() 
        except:
            sku_id = None

        try:
            product_name = response.css('h1[itemprop="name"]::text').get() 
        except Exception as e:
            product_name = ''

        try:
            product_url = response.css('link[itemprop="url"]::attr(href)').get()
        except Exception as e:
            product_url = ''

        try:
            image_url = response.css('img.product-gallery-image::attr(src)').get() 
        except Exception as e:
            image_url = ''

        try:
            product_description = get_product_description(response)
        except Exception as e:
            product_description = ''

        try:
            size = get_size_from_title(product_name)
        except:
            size = None

        item['website_id'] = WEBSITE_ID
        item['scrape_date'] = scrape_date
        item['category'] = self.category
        item['sub_category'] = self.sub_category
        item['brand'] = None
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
        item['qty_left'] = None
        item['usd_price'] = None
        item['usd_mrp'] = None
        item['miscellaneous'] = None

        yield item


'''
check page rendering
solve catalogue problem by keeping offset of 50 pages
mrp is not getting scraped
also check if last page has to be put or round number works for last iteration of product links loop
'''