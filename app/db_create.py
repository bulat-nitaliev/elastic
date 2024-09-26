
import psycopg2
from lxml import etree


def create_db(path:str, url:str, query:str):
    d = []
    for _, elem in etree.iterparse(path, tag='category'):
        l = elem.values()   
        l.append(elem.text)
        d.append(l)
    with psycopg2.connect(url)as conn:
        q = '''CREATE TABLE IF NOT EXISTS public.product (category_id varchar(50), parent_id varchar(50), name varchar(200)) '''
        cur = conn.cursor()
        cur.execute(q)
        cur.executemany('''insert into product values(%s, %s, %s)''', d[1:])
        cur.execute(query)
        print('success')


def generate_data(data, name):   
    actions = [
                    {
                    "_index" : name,
                    "_source" : node
                    }
                for node in data
                ]
    yield from actions

def update_sku(url,es):
    lst_example = []
    with psycopg2.connect(url)as conn:
        cursor = conn.cursor()
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
            response = es.search(index='sku', body=query)
            similar_uuids = [hit['_id'] for hit in response['hits']['hits'][:5]]
            # # print(similar_uuids)
            # l = tuple([i for _, i in similar_uuids])
        
            # q = f'''select uuid, title from sku where uuid in {l}'''
            # cursor.execute(q)
            # res = cursor.fetchall()
            
            


            # Обновление поля similar_sku
            cursor.execute("""
            UPDATE public.sku SET similar_sku = %s WHERE uuid = %s
            """, (similar_uuids, product_uuid))