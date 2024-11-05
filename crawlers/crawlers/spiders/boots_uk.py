from email import header
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import clean_name, get_size_from_title, clean_product_description, SCRAPER_URL, visited_skus, BENCHMARK_DATE, SCRAPE_DATE, visited_model_ids
import datetime
import requests

WEBSITE_ID = 10
PASSKEY = '324y3dv5t1xqv8kal1wzrvxig'
PAGE_SIZE = 180

sub_category_id_map = {
    'fragrance': '1952179',
    'skincare': '2300180',
    'face': '1595098',
    'eyes': '1595096',
    'lips': '1595099',
    'nails': '1595037',
    'make-up-gift-sets': '1604104',
    'glitter-accessories': '1871680',
    'palettes': '1595217',
    'make-up-remover-': '1595214'
}

headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'adrum': 'isAjax:true',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.boots.com',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

def get_product_description(response):
    product_description_raw_list = []

    main_description_list = [
        'div#contentOmnipresent p::text',
        'div#contentOmnipresent p span::text',
        'div#contentOmnipresent div::text',
        'div#contentOmnipresent ul li::text',
        'div#contentOmnipresent ul li span::text'
    ]

    additional_description_list = [
        'div#contentCollapse p::text',
        'div#contentCollapse p span::text',
        'div#contentCollapse div::text',
        'div#contentCollapse ul li::text',
        'div#contentCollapse ul li span::text'
    ]

    for md in main_description_list:
        desc_list = response.css(md).getall()
        product_description_raw_list.append(desc_list)

    for ad in additional_description_list:
        desc_list = response.css(ad).getall()
        product_description_raw_list.append(desc_list)

    product_description_list = [item for pd in product_description_raw_list for item in pd]
    product_description = '\n'.join(product_description_list)

    return product_description

class WdcSpider(scrapy.Spider):
    name = "scrape_boots_uk"
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
        manual_benchmark_date = (datetime.date.today().replace(day=1)).strftime("%Y-%m-%d")
        visited_sku_list = visited_model_ids(WEBSITE_ID,manual_benchmark_date,sub_category)

        url = f"https://www.boots.com/ProductListingViewRedesign?ajaxStoreImageDir=%2Fwcsstore%2FeBootsStorefrontAssetStore%2F&searchType=1000&advancedSearch=&cm_route2Page=&filterTerm=&storeId=11352&cm_pagename=&manufacturer=&sType=SimpleSearch&metaData=&catalogId=28501&searchTerm=&resultsPerPage=24&filterFacet=&resultCatEntryType=&gridPosition=&emsName=&disableProductCompare=false&langId=-1&facet=&categoryId={sub_category_id}"

        payload = f'contentBeginIndex=0&pageNo=&productBeginIndex=0&beginIndex=0&orderBy=&facetId=&pageView=grid&resultType=both&orderByContent=&searchTerm=&facet=&facetLimit=&minPrice=&maxPrice=&pageSize={PAGE_SIZE}&prem=&article=&storeId=11352&catalogId=28501&langId=-1&objectId=_6_3074457345618283155_3074457345619405964&requesttype=ajax'
        
        yield scrapy.Request(
            method="POST",
            url=url, 
            callback=self.parse_catalogue_pages,
            headers=headers,
            body=payload,
            meta = {'catalogue_url': url,'visited_sku_list':visited_sku_list},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.boots.com/no7-intense-volume-waterproof-mascara-10143391'
        # metadata = {}
        # final_url = SCRAPER_URL + url
        # metadata['category'] = category
        # metadata['sub_category'] = sub_category
        # metadata['product_url'] = url
        # yield scrapy.Request(url=final_url, callback=self.parse_product,meta={'metadata':metadata})

    def parse_catalogue_pages(self, response):
        catalogue_url = response.meta['catalogue_url']
        visited_sku_list = response.meta['visited_sku_list']

        product_count_string = response.css('span.showing_products_total::text').get().replace(',','')

        if product_count_string:
            print("====PRODUCT COUNT====",product_count_string)
            no_of_pages = int(product_count_string)//PAGE_SIZE + 1

            # no_of_pages = 1 ## For Testing
            offset = 0

            for page_index in range(1,no_of_pages+1):
                catalogue_links_url = catalogue_url
                payload = f'contentBeginIndex=0&pageNo={page_index}&productBeginIndex={offset}&beginIndex={offset}&orderBy=&facetId=&pageView=grid&resultType=both&orderByContent=&searchTerm=&facet=&facetLimit=&minPrice=&maxPrice=&pageSize={PAGE_SIZE}&prem=&article=&storeId=11352&catalogId=28501&langId=-1&objectId=_6_3074457345618283155_3074457345619405964&requesttype=ajax'
                offset += PAGE_SIZE

                yield scrapy.Request(
                    method="POST",
                    url=catalogue_links_url, 
                    callback=self.parse_catalogue_links, 
                    headers=headers,
                    body=payload,
                    meta={'page_index':page_index,'visited_sku_list':visited_sku_list}, 
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):
        page_index = response.meta['page_index']
        visited_sku_list = response.meta['visited_sku_list']

        product_cards = response.css('ul.grid_mode.grid li')
        for product in product_cards:
            product_link = product.css('a.product_name_link.product_view_gtm::attr(href)').get()
            model_number = product.css('div.estore_product_container::attr(data-productid)').get().replace('.P','')

            metadata = {}
            metadata['product_url'] = product_link
            metadata['page_index'] = page_index
            metadata['category'] = self.category
            metadata['sub_category'] = self.sub_category
            final_url = SCRAPER_URL + product_link
            if model_number not in visited_sku_list:
                yield scrapy.Request(url=final_url, callback=self.parse_product, meta={'metadata':metadata})
            else:
                print("------------- PRODUCT EXISTS ---------------")

    def parse_product(self, response):

        item = {}
        metadata_info = response.meta['metadata']
        product_link = metadata_info['product_url']

        miscellaneous = {}
  
        try:
            brand = response.css('span[itemprop="Brand"]::text').get().strip()
        except Exception as e:
            brand = ''

        try:
            product_name = response.css('div#estore_product_title h1::text').get().strip()
            if not product_name:
                product_name = ' '.join(response.css('div#estore_product_title h1 span::text').getall())
        except Exception as e:
            product_name = None

        try:
            master_sku_id = response.css('div#productId::text').get()
            miscellaneous['master_sku_id'] = master_sku_id
        except:
            master_sku_id = None

        try:
            product_url = product_link
        except Exception as e:
            product_url = ''

        size_info = response.css('div.details.details_redesign::text').get()

        try:
            size = size_info.split('|')[0].strip()
            if not size.strip():
                size = get_size_from_title(product_name)
        except:
            try:
                size = get_size_from_title(product_name)
            except:
                size = 'Not found'

        try:
            size_cost_relation = size_info.split('|')[1].strip().replace('£','')
            # size_cost_relation = size_info.split('|')[1].strip()
        except:
            size_cost_relation = 'Not found'

        miscellaneous['size_cost_relation'] = size_cost_relation

        try:
            mrp = response.css('div.was_price.was_price_redesign::text').get().split('£')[1]
        except:
            mrp = None

        try:
            price = response.css('div#PDP_productPrice::text').get().replace('£','')
        except:
            price = None


        try:
            discount = response.css('div.saving.saving_redesign::text').get().replace('\xa0',' ')
        except Exception as e:
            discount = None


        try:
            product_id = response.css('div::attr(data-bv-product-id)').get()
        except:
            product_id = None

        miscellaneous['product_id'] = product_id
        
        try:
            product_description = get_product_description(response)
            product_description = clean_product_description(product_description)
            product_description = product_description.strip()
        except Exception as e:
            product_description = ''

        review_api_url = f'https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey={PASSKEY}&productid={product_id}&contentType=reviews,questions&reviewDistribution=primaryRating,recommended&rev=0&contentlocale=en_EU,en_GB,en_IE,en_US,en_CA'
        review_response_string = requests.request("GET", review_api_url)

        try:
            review_response_json = json.loads(review_response_string.text)

            try:
                number_of_reviews = review_response_json['reviewSummary']['numReviews']
            except:
                number_of_reviews = 0

            miscellaneous['reviewCount'] = number_of_reviews

            try:
                avg_rating = review_response_json['reviewSummary']['primaryRating']['average']
            except:
                avg_rating = 0

            miscellaneous['avg_rating'] = avg_rating

        except Exception as e:
            print("Reviews Exception",e)
            pass


        # This will yield new records for each new colour
        variant_object_dict = {}

        try:
            variant_object_string = response.xpath("//script[contains(.,'productVarientsObject')]/text()")[0].extract()
            variant_object_string_clean = variant_object_string.replace(' var productVarientsObject = ','').replace(';','')

            variant_object_dict = json.loads(variant_object_string_clean)

        except:
            variant_object_dict = {}

        if variant_object_dict:
            for variant_key, variant_value in variant_object_dict.items():
                sku_id = variant_value.get("productCode","")
                colorName = variant_value.get("colorName","")
                catantry_id = variant_value.get("variantId","")
                image_url = f"https://boots.scene7.com/is/image/Boots/{sku_id}H"

                miscellaneous['colour'] = colorName
                miscellaneous['catantry_id'] = catantry_id

                stock_api_url = 'https://www.boots.com/campaign-list-twenty-six/AjaxBootsStockCheck?storeId=11352'
                payload= f'catentryId={catantry_id}&checkLocation=PDP&requesttype=ajax&exists=function%2520(x)%2520%257B%250A%2520%2520%2520%2520for%2520(var%2520i%2520%253D%25200%253B%2520i%2520%253C%2520this.length%253B%2520i%252B%252B)%2520%257B%250A%2520%2520%2520%2520%2520%2520%2520%2520if%2520(this%255Bi%255D%2520%253D%253D%2520x)%2520return%2520true%253B%250A%2520%2520%2520%2520%257D%250A%2520%2520%2520%2520return%2520false%253B%250A%257D&remove=function(from%252C%2520to)%2520%257B%250A%2520%2520var%2520rest%2520%253D%2520this.slice((to%2520%257C%257C%2520from)%2520%252B%25201%2520%257C%257C%2520this.length)%253B%250A%2520%2520this.length%2520%253D%2520from%2520%253C%25200%2520%253F%2520this.length%2520%252B%2520from%2520%253A%2520from%253B%250A%2520%2520return%2520this.push.apply(this%252C%2520rest)%253B%250A%257D'

                headers = {
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9',
                    'adrum': 'isAjax:true',
                    'content-length': '582',
                    'content-type': 'application/x-www-form-urlencoded',
                    'origin': 'https://www.boots.com',
                    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                }

                stock_response_string = requests.request("POST", stock_api_url, headers=headers, data=payload)
                stock_response_string_clean = stock_response_string.text.replace('/','').replace('*','')

                try:
                    stock_response_json = json.loads(stock_response_string_clean)

                    qty_left = stock_response_json.get('availableQuantity',-1)
                    is_available = stock_response_json.get('isStockAvailable',-1)

                    if is_available:
                        oos = 0
                    else:
                        oos = 1

                except:
                    oos = 'Not found'
                    qty_left = -2


                miscellaneous_string = json.dumps(miscellaneous)

                item['website_id'] = WEBSITE_ID
                item['scrape_date'] = SCRAPE_DATE
                item['category'] = metadata_info['category']
                item['sub_category'] = metadata_info['sub_category']
                item['brand'] = brand
                item['sku_id'] = sku_id
                item['product_name'] = product_name
                item['product_url'] = product_url
                item['image_url'] = image_url
                item['product_description'] = product_description
                item['info_table'] = None
                item['out_of_stock'] = 1 if oos else 0
                item['price'] = price
                item['mrp'] = mrp
                item['high_street_price'] = None
                item['discount'] = discount
                item['size'] = size
                item['qty_left'] = qty_left
                item['usd_price'] = None
                item['usd_mrp'] = None
                item['miscellaneous'] = miscellaneous_string

                yield item 

        else:
            try:
                oos_string = response.css('div#sold_out_text::attr(style)').get()
                if 'block' in oos_string or not oos_string:
                    oos = 1
                else:
                    oos = 0
            except:
                oos = 'Error'
                
            try:
                sku_id = master_sku_id
            except:
                sku_id = None

            try:
                model_number = response.css('input#gtmProdId::attr(value)').get().replace('.P','')
            except:
                model_number = None

            miscellaneous['model_number'] = model_number

            try:
                image_url = f'https://boots.scene7.com/is/image/Boots/{model_number}'
            except Exception as e:
                image_url = ''


            miscellaneous_string = json.dumps(miscellaneous)

            item['website_id'] = WEBSITE_ID
            item['scrape_date'] = SCRAPE_DATE
            item['category'] = metadata_info['category']
            item['sub_category'] = metadata_info['sub_category']
            item['brand'] = brand
            item['sku_id'] = sku_id
            item['product_name'] = product_name
            item['product_url'] = product_url
            item['image_url'] = image_url
            item['product_description'] = product_description
            item['info_table'] = None
            item['out_of_stock'] = 1 if oos else 0
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


'''
for boots you need end point and not entire url
'''