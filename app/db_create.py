
import psycopg2
import uuid
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
            similar_uuids = [uuid.UUID(hit['_source']['uuid']) for hit in response['hits']['hits'][:5]]
            q_s = f"""
            UPDATE public.sku SET similar_sku = ARRAY{similar_uuids} WHERE uuid = '{product_uuid}'  """
            
            cursor.execute(q_s)
            

# class DictIterator:
#     def __init__(self):
#         self.data = []  

#     def add(self, new_dict):
#         """Добавляет новый словарь в итератор."""
#         self.data.append(new_dict)

#     def __iter__(self):
#         """Возвращает итератор."""
#         self.index = 0  
#         return self

#     def __next__(self):
#         """Возвращает следующий элемент или вызывает StopIteration."""
#         if self.index < len(self.data):
#             result = self.data[self.index]
#             self.index += 1
#             return result
#         else:
#             raise StopIteration