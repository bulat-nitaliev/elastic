import datetime
import time
import uuid
import json
import psycopg2
from lxml import etree
from elasticsearch import Elasticsearch 
import pandas as pd
import psycopg2.extras
from s_p import query_create, query_insert_sku, q_sku
from decouple import config
from db_create import create_db, generate_data, update_sku
from elasticsearch.helpers import bulk




path = config('PATH_XML')
url = config('SQL_URL')
url_elastic = config('ELASTICSEARCH_URL')
es = Elasticsearch(url_elastic)
start = time.perf_counter()

print(path)
count, lst_data =  0, []
# create_db(path, url, query_create)
with psycopg2.connect(url)as conn:
    cur = conn.cursor()
    try:
        cur.execute(q_sku)
    except Exception as e:
        print(e)
        create_db(path, url, query_create)
         
for event, elem in etree.iterparse(path, tag='offer'):
        params = {}
        for param_elem in elem.findall("param"):
            param_name = param_elem.get("name")
            param_value = param_elem.text
            params[param_name] = param_value

        features_json = json.dumps(params)

        data = {
            "uuid": str(uuid.uuid4()),
            "marketplace_id": elem.get("marketplace_id"),
            "product_id": elem.findtext("id"),
            "title": elem.findtext('name') ,
            "description": elem.findtext('description', ''),
            "brand": elem.findtext('vendor'),
            "seller_id": elem.findtext('seller_id'),
            "seller_name": "",
            "first_image_url": elem.findtext('picture'),
            "category_id": elem.findtext('categoryId'),
            "category_lvl_1": "",
            "category_lvl_2": "",
            "category_lvl_3": "",
            "category_remaining": "",
            "features": features_json,
            "rating_count": (
                elem.get("rating_count")
                if elem.get("rating_count") is not None
                else None
            ),
            "rating_value": (
                elem.get("rating_value")
                if elem.get("rating_value") is not None
                else None
            ),
            "price_before_discounts": elem.findtext('oldprice') ,
            "discount": (
                elem.get("discount")
                if elem.get("discount") is not None
                else None
            ),
            "price_after_discounts": elem.findtext('price'),
            "bonuses": (
                elem.get("bonuses")
                if elem.get("bonuses") is not None
                else None
            ),
            "sales": (
                elem.get("sales") if elem.get("sales") is not None else None
            ),
            "inserted_at": (
                datetime.utcfromtimestamp(int(elem.findtext('inserted_at')))
                if elem.get("inserted_at") is not None
                else None
            ),
            "updated_at": (
                datetime.utcfromtimestamp(int(elem.findtext('modified_time')))
                if elem.get("modified_time") is not None
                else None
            ),
            "currency": elem.findtext('currencyId') ,
            "barcode": (
                elem.findtext('barcode') 
                if elem.findtext('barcode') is not None
                else None
            ),
        }
        
        # l = DictIterator()
        lst_data.append(data)
        
        if len(lst_data) == 100000:
            count += 100000
            print(count, 'read -> write')
            df = pd.DataFrame(lst_data)
            json_str = df.to_json(orient='records')

            json_records = json.loads(json_str)
            success, _ = bulk(es, generate_data(json_records, 'sku'))
            print(f"Успешно записано {success} документов.")
            with psycopg2.connect(url)as conn:
                cur = conn.cursor()
                l_l = (tuple(v.values()) for v in lst_data)
                lst_data = []
                psycopg2.extras.execute_batch(cur, query_insert_sku, l_l, page_size=1000)
                


df = pd.DataFrame(lst_data)
json_str = df.to_json(orient='records')

json_records = json.loads(json_str)
success, _ = bulk(es, generate_data(json_records, 'sku'))
print(f"Успешно записано {success} документов.")

with psycopg2.connect(url)as conn:
    cur = conn.cursor()
    l_l = (tuple(v.values()) for v in lst_data)
    psycopg2.extras.execute_batch( cur, query_insert_sku, l_l, page_size=1000)
   
count += len(lst_data)
print(count, 'read -> write')

          

print('update sku')
update_sku(url, es)

dt_end = time.perf_counter()
print('success ---- time ', round(dt_end-start, 2)/60) 