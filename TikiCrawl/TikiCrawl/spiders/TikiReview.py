from typing import Iterable
import scrapy
import re
import json
import logging

class TikireviewSpider(scrapy.Spider):
    name = "TikiReview"
    allowed_domains = ["tiki.vn"]
    start_urls = ["https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"]
    custom_settings = {
        'CONCURRENT_REQUESTS': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    }

    def parse(self, response):
        category_id_list = re.findall(r'/c(\d+)"', response.text)

        for category_id in category_id_list:
            for page in range(1, 51):
                url_category = f'https://tiki.vn/api/personalish/v1/blocks/listings?category={category_id}&page={page}'
                yield scrapy.Request(url=url_category, callback=self.parse_page)

    def parse_page(self, response):
        product_id = re.findall(r'{"id":(\d+)', response.text)
        # product_id = re.findall('{"id":(\d+)', response.text)
        for id in product_id:
            url_product = f'https://tiki.vn/api/v2/reviews?product_id={id}&include=comments&limit=20'
            yield scrapy.Request(url=url_product, callback=self.parse_comment, cb_kwargs={'id': id})

    def parse_comment(self, response, id):
        if response.status == 200:
            try:
                data_product = json.loads(response.text)
                data_product['product_id'] = id
                yield data_product
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON Decode Error: {e}: {response.url}")
        else:
            self.logger.error(f'Status Code {response.status} for product ID {id}')
