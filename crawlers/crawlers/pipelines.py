# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
try:
    from crawlers.storage import connect
except:
    from storage import connect

class FactivePipeline:
    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.conn = connect()

    def process_item(self,item,spider):
        self.store_db(item)
        return item
 
    def store_db(self,item):
        
        self.cursor = self.conn.cursor()
        insert_sql = f"""
        insert ignore into factive_data(
            website_id,scrape_date,category,sub_category,brand,
            sku_id,product_name,price,mrp,
            out_of_stock,discount,size,
            qty_left,product_description,info_table,product_url,
            image_url,high_street_price,usd_mrp,usd_price,miscellaneous
        ) VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s
        )
        """

        try:
            self.cursor.execute(insert_sql,(
                item['website_id'],item["scrape_date"],item['category'],item['sub_category'],item['brand'],
                item['sku_id'],item['product_name'],item['price'],item['mrp'],
                item['out_of_stock'],item['discount'],item['size'],
                item['qty_left'],item['product_description'],item['info_table'],item['product_url'],
                item['image_url'],item['high_street_price'],item['usd_mrp'],item['usd_price'],item['miscellaneous']
            ))

            self.conn.commit()   

        except Exception as e:
            print("============ERROR===============")
            print("Database Write Exception =>",e)

        self.cursor.close()

    def close_spider(self,spider):
        self.conn.close()


class FactiveUpdatePipeline:
    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.conn = connect()

    def process_item(self,item,spider):
        self.store_db(item)
        return item
 
    def store_db(self,item):

        self.cursor = self.conn.cursor()
        update_sql = f"""
        UPDATE factive_data 
        SET price=%s
        WHERE sku_id=%s
        AND website_id = 6
        AND scrape_date = curdate()
        """

        try:
            self.cursor.execute(update_sql,(
                item['price'], item['sku_id']
            ))

            self.conn.commit()   

        except Exception as e:
            print("Database Write Exception =>",e)

        self.cursor.close()

    def close_spider(self,spider):
        self.conn.close()