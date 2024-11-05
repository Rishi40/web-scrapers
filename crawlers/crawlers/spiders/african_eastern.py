import scrapy
from worldduty.items import FactiveItem
from worldduty.common_functions import SCRAPER_URL, visited_skus, BENCHMARK_DATE, write_to_log
from datetime import datetime
from scrapy import signals

WEBSITE_ID = 47

def get_product_description(response):
    main_description_list = []
    description_selectors_list = [
        'p font::text',
        'p b::text',
        'p::text',
        'p span::text',
        'p i::text'
    ]

    for i,selector in enumerate(description_selectors_list):
        product_description_list = response.css(selector).getall()
        product_description = ' '.join(product_description_list)
        if product_description:
            main_description_list.append(product_description)
            
    return ' '.join(main_description_list)

class WdcSpider(scrapy.Spider):
    name = "scrape_african_eastern"
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

        url = f'https://www.africanandeastern.com/{sub_category}'
        catalog_url = SCRAPER_URL + url
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)

        yield scrapy.Request(
            url=catalog_url, 
            callback=self.parse_catalogue_pages,
            meta = {'visited_sku_list':visited_sku_list,'sub_category':sub_category},
            dont_filter=True
        )

        # For Testing
        # url = 'https://www.africanandeastern.com/wine/bellingham-homestead-chardonnay-2'
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
        visited_sku_list = response.meta['visited_sku_list']
        sub_category = response.meta['sub_category']

        try:
            last_product_url = response.css('ul.pagination li a::attr(href)').getall()[-1]
            no_of_pages_string = last_product_url.split('?')[-1].split('=')[-1]
            no_of_pages = int(no_of_pages_string)
            print("NO OF PAGES ====", no_of_pages)

            # no_of_pages = 1 ## For Testing
            for page_index in range(1,no_of_pages+1):
                url = f'https://www.africanandeastern.com/{sub_category}?page={page_index}'

                page_url = SCRAPER_URL + url
                yield scrapy.Request(
                    url=page_url, 
                    callback=self.parse_catalogue_links,
                    meta = {'visited_sku_list':visited_sku_list},
                    dont_filter=True
                )

        except Exception as e:
            url = f'https://www.africanandeastern.com/{sub_category}'

            page_url = SCRAPER_URL + url
            yield scrapy.Request(
                url=page_url, 
                callback=self.parse_catalogue_links,
                meta = {'visited_sku_list':visited_sku_list},
                dont_filter=True
            )

    def parse_catalogue_links(self, response):

        product_cards = response.css('div.featured-box')
        visited_sku_list = response.meta['visited_sku_list']

        if product_cards:
            for product in product_cards:
                product_link = product.css('a.featured-link::attr(href)').get() 

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

        item = FactiveItem()
        metadata = response.meta['metadata']
        product_url = metadata['product_url']

        scrape_date = datetime.today().strftime('%Y-%m-%d')
        product_container = response.css('section#detailed-inner div.container')
        sku_id = None
        size = None

        try:
            product_name = product_container.css('div.product-name::text').get().strip()
        except Exception as e:
            product_name = None

        try:
            price = product_container.css('span.price-new::text').get().split()[-1]
        except Exception as e:
            price = None

        try:
            mrp = product_container.css('span.price-old::text').get().split()[-1]
        except Exception as e:
            mrp = None

        try:
            image_url = product_container.css('div.slider.slider-for div img::attr(src)').get()
        except Exception as e:
            image_url = None

        product_description_list = []
        try:
            product_description_objects = product_container.css('div#descript div p')
            for product_description_object in product_description_objects:
                product_description_string = get_product_description(product_description_object)
                product_description_list.append(product_description_string)
        except Exception as e:
            print("Exception--->",e)
            product_description = None

        product_description = ' | '.join(product_description_list)

        try:
            availability_string = product_container.css('button#button-cart::text').get() 
            if 'out of stock' in availability_string.lower():
                oos = 1
            else:
                oos = 0
        except:
            oos = None

        try:
            table_data = product_container.css('div#specif div p')
            more_info_list = []

            try:
                for data in table_data:
                    key = data.css('strong::text').get().strip()
                    value = data.css('span::text').get().strip()
                    if 'code' in key.lower():
                        sku_id = value
                    elif 'packing' in key.lower():
                        size = value
                    dict_string = key + " " + value
                    more_info_list.append(dict_string)
            except:
                more_info_list = []
        
        except:
            pass

        more_info_string = ' | '.join(more_info_list)

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
        item['info_table'] = more_info_string
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
