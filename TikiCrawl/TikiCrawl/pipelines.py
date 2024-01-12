import scrapy
import json
from kafka import KafkaProducer

import pymongo

class KafkaPipeline():
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def process_item(self, item, spider):
        self.producer.send('product-tiki', item)
        return item

class MongoDBPipeline(object):
    def __init__(self):
        super().__init__()
        self.client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = self.client["Tiki"]
        self.collection = self.db["review"]

    def process_item(self, item, spider):
        data = item
        self.collection.insert_one(data)
        return item