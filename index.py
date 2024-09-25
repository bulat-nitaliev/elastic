import os
import time
import uuid
import json
import psycopg2
from lxml import etree
from elasticsearch import Elasticsearch
from typing import List, Dict, Any
import pandas as pd

from sqlalchemy import create_engine
import urllib

path='C:\\Users\\Nitaliev_BB\\Desktop\\elektronika_products_20240924_135723.xml'

data = {}
# for event, elem in etree.iterparse(path, tag='offer'):
    
#     barcode = elem.findtext('barcode') 
#     categoryId = elem.findtext('categoryId') 
#     currencyId = elem.findtext('currencyId')  
#     description = elem.findtext('description', '')  
#     group_id = elem.findtext('group_id')  

#     modified_time = elem.findtext('modified_time')  
#     name = elem.findtext('name') 
#     param = elem.findtext('param')  
#     picture = elem.findtext('picture')
#     price = elem.findtext('price')  # Замените 'name' на ваш тег
#     url = elem.findtext('url')
#     vendor = elem.findtext('vendor')

#     print(barcode, barcode1,  categoryId, currencyId, description, group_id, modified_time, name, param, picture, price , url, vendor)
#     time.sleep(2)
#     print()
# d = []
# for _, elem in etree.iterparse(path, tag='category'):
#     l = elem.values()   
#     l.append(elem.text)
#     d.append(l)
# print('d -----', d)
with psycopg2.connect('postgresql://postgres:postgres@localhost:5484/test_db')as conn:
    # q = '''CREATE TABLE IF NOT EXISTS public.product (category_id varchar(50), parent_id varchar(50), name varchar(200)) '''
    cur = conn.cursor()
    q = 'select * from product'
    cur.execute(q)
    res = cur.fetchall()
    print(res)
#     cur.executemany('''insert into product values(%s, %s, %s)''', d[1:])
#     print('success')
    

# try:
#     # пытаемся подключиться к базе данных
#     conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5484/test_db')
#     print('success')
# except:
#     # в случае сбоя подключения будет выведено сообщение  в STDOUT
#     print('Can`t establish connection to database')


#     marketplace_id
# product_id
# title
# description
# brand
# seller_id
# seller_name
# first_image_url
# category_id
# features
# rating_count
# rating_value
# price_before_discounts
# discount
# price_after_discounts
# bonuses
# sales
# currency
# barcode

# offer {'id': '101464306305', 'available': 'true'}
# barcode {}
# barcode {}
# categoryId {}
# currencyId {}
# description {}
# group_id {}
# modified_time {}
# name {}
# picture {}
# price {}
# url {}
# vendor {}
# offer {'id': '1