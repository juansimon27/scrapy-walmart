# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from sqlalchemy.orm import sessionmaker
from models import Product, BranchProduct
from database_setup import engine
from models import Base


class StoragePipeline:
    def __init__(self, db_engine=engine):
        self.engine = db_engine
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):
        session = self.Session()
        # Check if the Product already exists
        product = (
            session.query(Product)
                .filter_by(store=item["store"], sku=item["sku"])
                .first()
        )

        if product is None:
            product = Product(store=item["store"], sku=item["sku"])

        product.barcodes = item["barcodes"]
        product.brand = item["brand"]
        product.name = item["name"]
        product.description = item["description"]
        product.image_url = item["image_url"]
        product.url = item['url']
        product.package = item['package']
        product.category = item['category']

        session.add(product)
        session.commit()

        # Check if the BranchProduct already exists
        branch_product = (
            session.query(BranchProduct)
                .filter_by(product=product, branch=item["branch"])
                .first()
        )

        if branch_product is None:
            branch_product = BranchProduct(product=product, branch=item["branch"])

        branch_product.stock = item["stock"]
        branch_product.price = item["price"]

        session.add(branch_product)
        session.commit()
        session.close()

        return item
