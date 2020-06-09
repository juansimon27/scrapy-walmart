from unittest import TestCase

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from models import Base, BranchProduct, Product


class ORMCreateSchemaTestCase(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")

    def test_create_schema(self):
        Base.metadata.create_all(self.engine)
        self.assertTrue(Base.metadata.tables[Product.__tablename__].exists(self.engine))
        self.assertTrue(
            Base.metadata.tables[BranchProduct.__tablename__].exists(self.engine)
        )


class ORMCreateExampleDataTestCase(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_example_product(self):

        some_product = Product(
            store="Walmart",
            barcodes="60538887928,1234567890,1234567891",
            sku="10295446",
            brand="Great Value",
            name="Spring Water",
            description="Convenient and refreshing, Great Value Spring Water is a healthy option that is Sodium-free and Non-carbonated. Bottled water is a quick and convenient way to fulfill your body's hydration needs. Zero calories, free from artifical flavors or colors, water is the right choice when it comes to your packaged beverage options. Eco-friendly, the bottle is made from 100% recycled plastic, a sustainable choice that is good for you and for the environment.",
            package="24 x 500ml",
            image_url="https://i5.walmartimages.ca/images/Large/887/928/999999-60538887928.jpg",
            category="Pantry, Household & Pets|Drinks|Water|Bottled Water",
            url="https://www.walmart.ca/en/ip/great-value-24pk-spring-water/6000143709667",
        )

        some_branch_product_1 = BranchProduct(
            branch="BRNCH", product=some_product, stock=123, price=2.27,
        )

        some_branch_product_2 = BranchProduct(
            branch="3106", product=some_product, stock=0, price=2.27,
        )

        self.session.add(some_product)
        self.session.commit()

        products = self.session.query(Product).all()
        self.assertEqual(len(products), 1)
        self.assertIn(some_product, products)

        branch_products = self.session.query(BranchProduct).all()
        self.assertEqual(len(branch_products), 2)
        self.assertIn(some_branch_product_1, branch_products)
        self.assertIn(some_branch_product_2, branch_products)

    def test_store_sku_unique(self):

        some_product_a = Product(store="STR01", sku="SKU001", name="Some Name",)

        some_product_b = Product(store="STR02", sku="SKU001", name="Some Name",)

        some_product_c = Product(store="STR01", sku="SKU001", name="Some Name",)

        self.session.add(some_product_a)
        self.session.add(some_product_b)
        self.session.commit()

        self.session.add(some_product_c)
        with self.assertRaises(IntegrityError):
            self.session.commit()
