import scrapy
import pandas as pd
from datetime import datetime

class BcfSpider(scrapy.Spider):
    name = "bcf"
    allowed_domains = ["www.bcf.com.au"]
    start_urls = ["https://www.bcf.com.au/"]

    def __init__(self, *args, **kwargs):
        super(BcfSpider, self).__init__(*args, **kwargs)
        self.items = []

    def parse(self, response):
        sublinks = response.xpath('//li[@class="shopbycategory"]//ul[@class="burger-subcategory-level-3"]//@href').getall()
        for link in sublinks:
            if 'http' in link:
                yield scrapy.Request(url=link, callback=self.product_details)

    def product_details(self, response):
        product_categories = response.xpath('//a[@class="breadcrumb-element "]//text()').getall()
        product_category = product_categories[0].strip()
        product_subcategory = product_categories[1].strip() if len(product_categories) > 1 else ""

        product_threads = response.xpath('//li[contains(@class,"grid-tile")]')
        for item in product_threads:
            items = {}
            product_id = item.xpath('./div[contains(@class,"product-tile")]/@data-itemid').get()
            product_url = item.xpath('.//a[contains(@class,"thumb-link")]/@href').get().strip()
            if 'http' not in product_url:
                product_url = 'https://www.bcf.com.au' + product_url
            product_name = item.xpath('.//div[@class="product-name"]/a/text()').get().strip()

            actual_price = item.xpath('.//span[@class="product-standard-price"]/text()[1]').get().strip().replace('$', '') if item.xpath('.//span[@class="product-standard-price"]/text()[1]') else ""
            discount_price = item.xpath('.//span[contains(@class,"product-sales-price")]/text()[1]').get().strip().replace('$', '') if item.xpath('.//span[contains(@class,"product-sales-price")]/text()[1]') else ""
            
            if discount_price == "":
                discount_price = item.xpath('.//span[@class="member-price"]/span[1]/text()').get().strip().replace('$', '') if item.xpath('.//span[@class="member-price"]/span[1]/text()') else ""
            if actual_price == "" and discount_price != "":
                actual_price, discount_price = discount_price, actual_price
            
            if actual_price == '' and discount_price == '':
                actual_price = item.xpath('.//span[contains(@class,"product-sales-price")]/text()[1]').get().strip()

            feature_of_promotion = item.xpath('.//div[@class="product-badge red promo-badge"]/text()').get() if item.xpath('.//div[@class="product-badge red promo-badge"]/text()') else ""

            items['Date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            items['Product Id'] = product_id
            items['Product Url'] = product_url
            items['Product Name'] = product_name
            items['Product Category'] = product_category
            items['Product SubCategory'] = product_subcategory
            items['Actual price'] = actual_price
            items['Discounted Price'] = discount_price
            items['Feature of Promotion'] = feature_of_promotion

            self.items.append(items)

        yield scrapy.Request(url=response.url, callback=self.parse_details)

    def parse_details(self, response):
        for item in self.items:
            item['Product Category'] = response.xpath('(//a[@class="breadcrumb-element"]//text())[1]').get().strip()
            item['Product SubCategory'] = response.xpath('(//a[@class="breadcrumb-element"]//text())[2]').get().strip()
            yield item

    def closed(self, reason):
        try:
            df = pd.DataFrame(self.items)
            df.to_excel(f'bcf_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.xlsx', index=False)
        except Exception as e:
            self.logger.error(f"Error saving data to Excel file: {e}")
