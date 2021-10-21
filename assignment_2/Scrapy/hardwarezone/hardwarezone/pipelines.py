# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo


class HardwarezonePipeline:
    def __init__(self):
        connection = pymongo.MongoClient("localhost", 27017)
        db = connection["hardwarezone"]
        self.collection = db["threads"]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            # If any of the item data is null, item will be drop
            if item[data] is None:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert(dict(item))
            return item
