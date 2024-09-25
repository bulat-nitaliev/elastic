import os
import uuid
import json
import psycopg2
from lxml import etree
from elasticsearch import Elasticsearch
from typing import List, Dict, Any
import pandas as pd

from sqlalchemy import create_engine
import urllib

# Подключение к PostgreSQL
def connect_db() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    quoted = urllib.parse.quote_plus(os.environ['DATABASE_URL'])
    engine = create_engine('postgresql+psycopg2://postgres:postgres@postgres/test_db', fast_executemany=True)
    return conn

# Подключение к Elasticsearch
def connect_elasticsearch() -> Elasticsearch:
    return Elasticsearch(os.environ['ELASTICSEARCH_URL'])

# Создание таблицы
def create_table(conn: psycopg2.extensions.connection):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.sku (
            uuid UUID PRIMARY KEY,
            marketplace_id INTEGER,
            product_id BIGINT,
            title TEXT,
            description TEXT,
            brand TEXT,
            seller_id INTEGER,
            seller_name TEXT,
            first_image_url TEXT,
            category_id INTEGER,
            category_lvl_1 TEXT,
            category_lvl_2 TEXT,
            category_lvl_3 TEXT,
            category_remaining TEXT,
            features JSON,
            rating_count INTEGER,
            rating_value DOUBLE PRECISION,
            price_before_discounts REAL,
            discount DOUBLE PRECISION,
            price_after_discounts REAL,
            bonuses INTEGER,
            sales INTEGER,
            inserted_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now(),
            currency TEXT,
            barcode BIGINT,
            similar_sku UUID[]
        );
        """)
        conn.commit()

# Загрузка данных из XML
def load_data_from_xml(conn: psycopg2.extensions.connection, es: Elasticsearch, path='C:\\Users\\Nitaliev_BB\\Desktop\\elektronika_products_20240924_135723.xml'):
    for event, elem in etree.iterparse(path, events=('end',), tag='offer'):
        data = {
            'uuid': str(uuid.uuid4()),
            'marketplace_id': int(elem.get('marketplaceId')),
            'product_id': int(elem.get('id')),
            'title': elem.findtext('name'),
            'description': elem.findtext('description'),
            'brand': elem.findtext('brand'),
            'seller_id': int(elem.findtext('sellerId')),
            'seller_name': elem.findtext('sellerName'),
            'first_image_url': elem.findtext('firstImageUrl'),
            'category_id': int(elem.findtext('categoryId')),
            'features': json.dumps({feature.tag: feature.text for feature in elem.findall('feature')}),
            'rating_count': int(elem.findtext('ratingCount', default=0)),
            'rating_value': float(elem.findtext('ratingValue', default=0)),
            'price_before_discounts': float(elem.findtext('priceBeforeDiscounts', default=0)),
            'discount': float(elem.findtext('discount', default=0)),
            'price_after_discounts': float(elem.findtext('priceAfterDiscounts', default=0)),
            'bonuses': int(elem.findtext('bonuses', default=0)),
            'sales': int(elem.findtext('sales', default=0)),
            'currency': elem.findtext('currency'),
            'barcode': int(elem.findtext('barcode', default=0)),
            'similar_sku': []
        }

        # Вставка в PostgreSQL
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO public.sku (uuid, marketplace_id, product_id, title, description, brand, seller_id, seller_name,
                                    first_image_url, category_id, features, rating_count, rating_value,
                                    price_before_discounts, discount, price_after_discounts, bonuses, sales,
                                    currency, barcode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (data['uuid'], data['marketplace_id'], data['product_id'], data['title'], data['description'],
                  data['brand'], data['seller_id'], data['seller_name'], data['first_image_url'],
                  data['category_id'], data['features'], data['rating_count'], data['rating_value'],
                  data['price_before_discounts'], data['discount'], data['price_after_discounts'],
                  data['bonuses'], data['sales'], data['currency'], data['barcode']))

        # Вставка в Elasticsearch
        es.index(index='products', id=data['uuid'], body=data)

        # Очистка элемента
        elem.clear()

# Поиск похожих товаров
def find_similar_products(conn: psycopg2.extensions.connection, es: Elasticsearch):
    with conn.cursor() as cursor:
        cursor.execute("SELECT uuid, title FROM public.sku")
        products = cursor.fetchall()

        for product in products:
            product_uuid, title = product
            query = {
                "query": {
                    "match": {
                        "title": title
                    }
                }
            }
            response = es.search(index='products', body=query)
            similar_uuids = [hit['_id'] for hit in response['hits']['hits'][:5]]

            # Обновление поля similar_sku
            cursor.execute("""
            UPDATE public.sku SET similar_sku = %s WHERE uuid = %s
            """, (similar_uuids, product_uuid))

        conn.commit()

if __name__ == "__main__":
    conn = connect_db()
    es = connect_elasticsearch()
    create_table(conn)
    load_data_from_xml(conn, es)
    find_similar_products(conn, es)
    conn.close()
