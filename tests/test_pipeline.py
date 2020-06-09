from unittest import TestCase, mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, BranchProduct, Product
from product_scraping.items import Product
from product_scraping.pipelines import StoragePipeline


class StoragePipelineTestCase(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self.spider = mock.Mock()
        self.item = Product(
            store="Walmart",
            barcodes="60538887928,1234567890,1234567891",
            sku="10295446",
            brand="Great Value",
            name="Spring Water",
            description="Convenient and refreshing, Great Value Spring Water is a healthy option that is Sodium-free and Non-carbonated. Bottled water is a quick and convenient way to fulfill your body's hydration needs. Zero calories, free from artifical flavors or colors, water is the right choice when it comes to your packaged beverage options. Eco-friendly, the bottle is made from 100% recycled plastic, a sustainable choice that is good for you and for the environment.",
            package="24 x 500ml",
            image_url="https://i5.walmartimages.ca/images/Large/887/928/999999-60538887928.jpg",
            branch="BRANCH01",
            stock=10,
            price=20,
        )

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_process_item_new_data(self):
        pipeline = StoragePipeline(db_engine=self.engine)
        pipeline.open_spider(self.spider)
        pipeline.process_item(self.item, self.spider)
        pipeline.close_spider(self.spider)

        product = (
            self.session.query(Product)
            .filter_by(store=self.item["store"], sku=self.item["sku"])
            .first()
        )
        self.assertIsNotNone(product)
        branch_product = (
            self.session.query(BranchProduct)
            .filter_by(product=product, branch=self.item["branch"])
            .first()
        )
        self.assertIsNotNone(branch_product)

    def test_process_item_existing_product(self):
        existing_product = Product(
            store=self.item["store"],
            sku=self.item["sku"],
            brand=self.item["brand"],
            name=self.item["name"],
            description=self.item["description"],
            package=self.item["package"],
            image_url=self.item["image_url"],
        )

        self.session.add(existing_product)
        self.session.commit()

        pipeline = StoragePipeline(db_engine=self.engine)
        pipeline.process_item(self.item, self.spider)

        product = (
            self.session.query(Product)
            .filter_by(store=self.item["store"], sku=self.item["sku"])
            .first()
        )
        self.assertEqual(product, existing_product)
        branch_product = (
            self.session.query(BranchProduct)
            .filter_by(product=product, branch=self.item["branch"])
            .first()
        )
        self.assertIsNotNone(branch_product)

    def test_process_item_existing_product_and_existing_branch_product(self):
        existing_product = Product(
            store=self.item["store"],
            sku=self.item["sku"],
            brand=self.item["brand"],
            name=self.item["name"],
            description=self.item["description"],
            package=self.item["package"],
            image_url=self.item["image_url"],
        )

        self.session.add(existing_product)
        self.session.commit()

        existing_branch_product = BranchProduct(
            product=existing_product,
            branch=self.item["branch"],
            stock=self.item["stock"],
            price=self.item["price"],
        )

        self.session.add(existing_branch_product)
        self.session.commit()

        pipeline = StoragePipeline(db_engine=self.engine)
        pipeline.process_item(self.item, self.spider)

        product = (
            self.session.query(Product)
            .filter_by(store=self.item["store"], sku=self.item["sku"])
            .first()
        )
        self.assertEqual(product, existing_product)
        branch_product = (
            self.session.query(BranchProduct)
            .filter_by(product=product, branch=self.item["branch"])
            .first()
        )
        self.assertEqual(branch_product, existing_branch_product)
