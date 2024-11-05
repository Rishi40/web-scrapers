import re
import time
import datetime
try:
    from crawlers.storage import connect
except:
    from storage import connect
try:
    from crawlers.keys import SCRAPER_KEY
except:
    from keys import SCRAPER_KEY
import json
import os

SCRAPER_URL = f'http://api.scraperapi.com/?api_key={SCRAPER_KEY}&url='
# BENCHMARK_DATE = (datetime.date.today().replace(day=25) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
BENCHMARK_DATE = (datetime.date.today() - datetime.timedelta(days=20)).strftime("%Y-%m-%d")
SCRAPE_DATE = datetime.date.today().strftime('%Y-%m-%d')
GLOBAL_RETRY_COUNT = 3

def clean_name(name):
    cleaned_name = name.replace('<p>','').replace('</p>','').replace('<br />','').replace('<br >','').replace("&rsquo;","'").replace('&reg;','®').replace("&#39;","'").replace("&nbsp;"," ").replace('&ldquo;','"').replace('&rdquo;','"').replace('&eacute;','é').replace('&quot;','"').replace('&lt;p&gt;','').replace('&lt;/p&gt;','').replace('&amp;','&').replace("&#39;","'").replace('&lt;','').replace('&gt;','').replace('strong','').replace('/strong','').replace('/',' ').replace('\xa0',' ')
    return cleaned_name

def clean_product_description(name):
    cleaned_name = name.replace("&rsquo;","'").replace('&reg;','®').replace("&#39;","'").replace("&nbsp;"," ").replace('&ldquo;','"').replace('&rdquo;','"').replace('&eacute;','é').replace('&quot;','"').replace('&lt;p&gt;','').replace('&lt;/p&gt;','').replace('&amp;','&').replace("&#39;","'").replace('&lt;','').replace('&gt;','').replace('strong','').replace('/strong','').replace('\xa0',' ').replace('\t',' ').replace('\n',' ').replace('<p>','').replace('</p>','').replace('<b>','').replace('</b>','').replace('<br>','').replace('<li>',' ').replace('<ul>',' ').replace('</li>',' ').replace('</ul>',' ')
    return cleaned_name

def get_size_from_title(product_name):
    try:
        if re.search('(\d+\.?[\d+\s]*ml)[\s+]*(\d+\.?[\d+\s]*ml)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*ml)[\s+]*(\d+\.?[\d+\s]*ml)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*pcs)[\s+]*(\d+\.?[\d+\s]*pcs)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*pcs)[\s+]*(\d+\.?[\d+\s]*pcs)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*g)[\s+]*(\d+\.?[\d+\s]*g)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*g)[\s+]*(\d+\.?[\d+\s]*g)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*kg)[\s+]*(\d+\.?[\d+\s]*kg)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*kg)[\s+]*(\d+\.?[\d+\s]*kg)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*oz)[\s+]*(\d+\.?[\d+\s]*oz)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*oz)[\s+]*(\d+\.?[\d+\s]*oz)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*cm)[\s+]*(\d+\.?[\d+\s]*cm)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*cm)[\s+]*(\d+\.?[\d+\s]*cm)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*cl)[\s+]*(\d+\.?[\d+\s]*cl)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*cl)[\s+]*(\d+\.?[\d+\s]*cl)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*l)[\s+]*(\d+\.?[\d+\s]*l)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*l)[\s+]*(\d+\.?[\d+\s]*l)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*pc)[\s+]*(\d+\.?[\d+\s]*pc)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*pc)[\s+]*(\d+\.?[\d+\s]*pc)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*lt)[\s+]*(\d+\.?[\d+\s]*lt)?',product_name,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*lt)[\s+]*(\d+\.?[\d+\s]*lt)?',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        else:
            size = None

        try:
            additional_size = regex_object.group(2)
            size += ', ' + additional_size
        except:
            additional_size = ''

    except Exception as e:
        size = None

    return size


def get_size_from_title_2(product_name):
    try:
        if re.search('(\d*[\s*x\s*]*\d*ml)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*ml)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*g)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*g)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*kg)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*kg)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*oz)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*oz)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*pcs)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*pcs)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*cm)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*cm)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*cl)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*cl)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d*[\s*x\s*]*\d*l)',product_name,re.IGNORECASE):
            regex_object = re.search('(\d*[\s*x\s*]*\d*l)',product_name,re.IGNORECASE)
            size = regex_object.group(1)
        else:
            size = 'N/A'

    except Exception as e:
        print("Size Exception: ",e)
        size = None

    return size

def get_size_from_product_description(product_description):
    try:
        if re.search('(\d+[\s]*x)[\s]*(\d+\.?[\d+\s]*ml)[\s]*(\d+\.?[\d+\s]*ml)?',product_description,re.IGNORECASE):
            regex_list = re.findall(r'(\d+[\s]*x)[\s]*(\d+\.?[\d+\s]*ml)[\s]*(\d+\.?[\d+\s]*ml)?',product_description,re.IGNORECASE)
            size = ' '.join(regex_list[0])
        elif re.search('(\d+[\s]*x)[\s]*(\d+\.?[\d+\s]*cl)[\s]*(\d+\.?[\d+\s]*cl)?',product_description,re.IGNORECASE):
            regex_list = re.findall(r'(\d+[\s]*x)[\s]*(\d+\.?[\d+\s]*cl)[\s]*(\d+\.?[\d+\s]*cl)?',product_description,re.IGNORECASE)
            size = ' '.join(regex_list[0])
        elif re.search('(\d+\.?[\d+\s]*ml)[\s+]*(\d+\.?[\d+\s]*ml)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*ml)[\s+]*(\d+\.?[\d+\s]*ml)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*g)[\s+]*(\d+\.?[\d+\s]*g)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*g)[\s+]*(\d+\.?[\d+\s]*g)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*kg)[\s+]*(\d+\.?[\d+\s]*kg)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*kg)[\s+]*(\d+\.?[\d+\s]*kg)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*pcs)[\s+]*(\d+\.?[\d+\s]*pcs)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*pcs)[\s+]*(\d+\.?[\d+\s]*pcs)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*cl)[\s+]*(\d+\.?[\d+\s]*cl)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*cl)[\s+]*(\d+\.?[\d+\s]*cl)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*l)[\s+]*(\d+\.?[\d+\s]*l)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*l)[\s+]*(\d+\.?[\d+\s]*l)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*pc)[\s+]*(\d+\.?[\d+\s]*pc)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*pc)[\s+]*(\d+\.?[\d+\s]*pc)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        elif re.search('(\d+\.?[\d+\s]*lt)[\s+]*(\d+\.?[\d+\s]*lt)?',product_description,re.IGNORECASE):
            regex_object = re.search('(\d+\.?[\d+\s]*lt)[\s+]*(\d+\.?[\d+\s]*lt)?',product_description,re.IGNORECASE)
            size = regex_object.group(1)
        else:
            size = 'N/A'

        try:
            additional_size = regex_object.group(2)
            size += ', ' + additional_size
        except:
            additional_size = ''

    except Exception as e:
        print("Size Exception: ",e)
        size = None

    return size

def select_web_page(cursor, website_id,sub_category):

    select_web_pages_object = f"""
        SELECT id,page_start,page_end
        FROM crawler_db.web_pages
        WHERE website_id = %s
        AND sub_category = %s
        AND is_picked = 0
        ORDER BY sub_category, page_start;
    """

    try:
        cursor.execute(select_web_pages_object,(website_id,sub_category))  
        select_web_pages = cursor.fetchone()

    except Exception as e:
        print("Database Read Exception =>",e)
        select_web_pages = ()
        
    return select_web_pages

def update_web_page(conn, cursor, web_page_id):

    update_web_pages_object = f"""
        UPDATE crawler_db.web_pages
        SET is_picked=1
        WHERE id = %s;
    """

    try:
        cursor.execute(update_web_pages_object,(web_page_id,))  
        conn.commit()   

    except Exception as e:
        print("Database Update Exception =>",e)
        conn.rollback()

def get_web_page(website_id,sub_category):
    conn = connect()
    cursor = conn.cursor()

    try:
        web_page_tuple = select_web_page(cursor,website_id,sub_category)
        web_page_id = web_page_tuple[0]

        if web_page_id:
            update_web_page(conn,cursor,web_page_id)

    except Exception as e:
        web_page_tuple = ()

    cursor.close()
    conn.close()
    return web_page_tuple

def visited_sku_ids(website_id,date,sub_category):
    conn = connect()
    cursor = conn.cursor()

    select_visited_skus_object = f"""
    SELECT sku_id
    FROM crawler_data 
    WHERE website_id = %s
    AND scrape_date >= %s
    AND sub_category = %s
    """

    try:
        cursor.execute(select_visited_skus_object,(website_id,date,sub_category))  
        select_visited_skus_tuple = cursor.fetchall()
        select_visited_skus = [sku for sku_tuple in select_visited_skus_tuple for sku in sku_tuple]
        return select_visited_skus

    except Exception as e:
        print("Database Read Exception =>",e)

    cursor.close()
    conn.close()

def visited_skus(website_id,date,sub_category):
    conn = connect()
    cursor = conn.cursor()

    select_visited_skus_object = f"""
    SELECT product_url
    FROM crawler_data 
    WHERE website_id = %s
    AND scrape_date >= %s
    AND sub_category = %s
    """

    try:
        cursor.execute(select_visited_skus_object,(website_id,date,sub_category))  
        select_visited_skus_tuple = cursor.fetchall()
        select_visited_skus = [sku for sku_tuple in select_visited_skus_tuple for sku in sku_tuple]
        return select_visited_skus

    except Exception as e:
        print("Database Read Exception =>",e)

    cursor.close()
    conn.close()


def visited_model_ids(website_id,date,sub_category):
    conn = connect()
    cursor = conn.cursor()

    select_miscellaneous_object = f"""
    SELECT miscellaneous
    FROM crawler_data 
    WHERE website_id = %s
    AND scrape_date >= %s
    AND sub_category = %s
    """

    try:
        select_model_ids = []
        cursor.execute(select_miscellaneous_object,(website_id,date,sub_category))  
        select_miscellaneous_tuple = cursor.fetchall()
        for miscellaneous_tuple in select_miscellaneous_tuple:
            miscellaneous_object = json.loads(miscellaneous_tuple[0])
            model_number = miscellaneous_object.get('model_number')
            if model_number:
                select_model_ids.append(model_number)

        return select_model_ids

    except Exception as e:
        print("Database Read Exception =>",e)

    cursor.close()
    conn.close()

def visited_miscelleneous_parameter(website_id,date,sub_category,misc_parameter):
    conn = connect()
    cursor = conn.cursor()

    select_miscellaneous_object = """
        SELECT miscellaneous
        FROM crawler_data 
        WHERE website_id = %s
        AND scrape_date >= %s
        AND sub_category = %s;
    """

    try:
        misc_parameter_list = []
        cursor.execute(select_miscellaneous_object,(website_id,date,sub_category))  
        select_miscellaneous_tuple = cursor.fetchall()
        for miscellaneous_tuple in select_miscellaneous_tuple:
            try:
                miscellaneous_object = json.loads(miscellaneous_tuple[0])
                misc_parameter_value = miscellaneous_object.get(misc_parameter)
                if misc_parameter_value not in misc_parameter_list:
                    misc_parameter_list.append(misc_parameter_value)
            except:
                continue

        return misc_parameter_list

    except Exception as e:
        print("Database Read Exception =>",e)

    cursor.close()
    conn.close()

def write_item_to_database(item):
    conn = connect()
    cursor = conn.cursor()

    insert_sql = f"""
    insert ignore into crawler_data(
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
        cursor.execute(insert_sql,(
            item['website_id'],item["scrape_date"],item['category'],item['sub_category'],item['brand'],
            item['sku_id'],item['product_name'],item['price'],item['mrp'],
            item['out_of_stock'],item['discount'],item['size'],
            item['qty_left'],item['product_description'],item['info_table'],item['product_url'],
            item['image_url'],item['high_street_price'],item['usd_mrp'],item['usd_price'],item['miscellaneous']
        ))
        conn.commit()   

    except Exception as e:
        print("============ERROR===============")
        print("Database Write Exception =>",e)

    cursor.close()
    conn.close()

## SELENIUM SECTION

def write_unresolved_products_to_file(unresolved_products_destination_filename,unresolved_products_list,sub_category):
    with open(unresolved_products_destination_filename, "a") as unresolved_products_destination_filename:
        unresolved_products_destination_filename.write(f"Unresolved list {sub_category} \n")
        unresolved_products_destination_filename.writelines(unresolved_products_list)

def get_product_batch(website_id,category,sub_category,date,sku_id):

    connection = connect()
    cursor = connection.cursor()

    fetch_product_batch = """
        SELECT sku_id,product_url
        FROM crawler_db.crawler_data
        WHERE website_id = %s
        AND category = %s
        AND sub_category = %s
        AND scrape_date >= %s
        AND sku_id > %s
        ORDER BY sku_id;
    """
    cursor.execute(fetch_product_batch,(website_id,category,sub_category,date,sku_id))
    product_batch_list = cursor.fetchall()

    cursor.close()
    connection.close()

    return product_batch_list

def get_product_batch_test(website_id,category,sub_category,date,sku_id):

    connection = connect()
    cursor = connection.cursor()

    fetch_product_batch = """
        SELECT sku_id,product_url
        FROM crawler_db.crawler_data
        WHERE website_id = %s
        AND category = %s
        AND sub_category = %s
        AND scrape_date >= %s
        AND sku_id > %s
        ORDER BY sku_id
        LIMIT 10;
    """
    cursor.execute(fetch_product_batch,(website_id,category,sub_category,date,sku_id))
    product_batch_list = cursor.fetchall()

    cursor.close()
    connection.close()

    return product_batch_list
    
def get_website_name(website_id):

    connection = connect()
    cursor = connection.cursor()

    fetch_website_name = """
        SELECT website_name
        FROM crawler_db.website_mapping
        WHERE website_id = %s;
    """
    cursor.execute(fetch_website_name,(website_id,))
    website_name = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return website_name

def reset_webPage_table():
    connection = connect()
    cursor = connection.cursor()

    reset_webPage = """
        UPDATE crawler_db.web_pages
        SET is_picked=0
        WHERE is_picked=1;
    """

    try:
        cursor.execute(reset_webPage)
        connection.commit()   
    except Exception as e:
        print("Database Update Exception =>",e)
        connection.rollback()

    cursor.close()
    connection.close()
 
def write_to_log(file_name,spider_name,stats):

    log_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "scraper_logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, file_name)
    
    with open(log_file, 'a+') as f:
        f.write(f"\nSpider Name: {spider_name}\n")
        f.write("Scrapy Stats:\n")
        for key, value in stats.items():
            f.write(f"{key}: {value}\n")

# name = "Latex Free Round Makeup Sponge - 2 pcs."
# product_id_list = visited_miscelleneous_parameter(10,'2023-02-26','fragrance')
# visited_sku_list = visited_miscelleneous_parameter(24,BENCHMARK_DATE,'fragrance','master_sku_id')
# print(visited_sku_list)