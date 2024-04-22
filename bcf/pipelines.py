# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BcfPipeline:
    def process_item(self, item, spider):
        return item
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class RemoveDuplicatesPipeline:
    def __init__(self):
        self.seen_items = set()

    def process_item(self, item, spider):
        item_id = item['Product Id']  # Replace 'id' with the appropriate field that represents a unique identifier for your items

        if item_id in self.seen_items:
            raise DropItem(f'Duplicate item found: {item_id}')
        else:
            self.seen_items.add(item_id)
            return item