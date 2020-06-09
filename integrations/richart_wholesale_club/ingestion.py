import os
import re
import pandas as pd
from database_setup import engine
from models import BranchProduct, Product, Base
from sqlalchemy.orm import sessionmaker

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")


def process_csv_files():
    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|")
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|")

    # Reshape & filtering
    prices_stock_df = prices_stock_df[prices_stock_df['STOCK'] > 0]
    prices_stock_df = prices_stock_df.loc[prices_stock_df['BRANCH'].isin(['MM', 'RHSM'])]
    products_df = products_df.fillna(value='')
    products_df['STORE'] = 'Richarts'
    products_df['URL'] = 'n/a'
    condition = products_df['SKU'].isin(prices_stock_df['SKU'].values)
    products_df = products_df[condition]
    # Taking the lowest pricing and the total stock availability
    branchproducts_df = prices_stock_df.groupby(['SKU', 'BRANCH'], as_index=False, sort=False,
                                                group_keys=False).aggregate( {'PRICE': 'min', 'STOCK': 'sum'})

    # Converting data types
    prices_stock_df['BRANCH'] = prices_stock_df['BRANCH'].astype('category')

    # Cleaning & styling
    products_df['DESCRIPTION'] = products_df['DESCRIPTION'].apply(lambda x: re.sub('<.*?>', '', x))
    products_df['CATEGORY'] = products_df['CATEGORY'].str.cat(
        [products_df['SUB_CATEGORY'], products_df['SUB_SUB_CATEGORY']], sep=' | ')
    products_df['CATEGORY'] = products_df['CATEGORY'].str.lower()
    products_df = products_df.drop(
        columns=['SUB_CATEGORY', 'SUB_SUB_CATEGORY', 'ORGANIC_ITEM', 'KIRLAND_ITEM', 'BUY_UNIT', 'FINELINE_NUMBER',
                 'DESCRIPTION_STATUS'])
    products_df['PACKAGE'] = products_df['DESCRIPTION'].apply(lambda x: re.findall(r'(?!0)\d+\s?\w+\.?$', x))
    products_df['PACKAGE'] = products_df['PACKAGE'].apply(''.join).apply(lambda x: x.replace('.', ''))
    products_df['NAME'] = products_df['NAME'].str.capitalize()
    products_df['BRAND'] = products_df['BRAND'].str.capitalize()
    products_df['DESCRIPTION'] = products_df['DESCRIPTION'].str.capitalize()
    products_df['PACKAGE'] = products_df['PACKAGE'].str.lower()

    products_df.columns = [column.lower() for column in products_df.columns]
    branchproducts_df.columns = [column.lower() for column in branchproducts_df.columns]

    # Loading to SQLite DB
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    session.bulk_insert_mappings(Product, products_df.to_dict(orient='records'))
    session.commit()

    sql_df = pd.read_sql("SELECT products.id, sku FROM products WHERE store = 'Richarts'", con=engine)
    sql_df['sku'] = sql_df['sku'].astype(int)

    branches_df = pd.merge(sql_df, branchproducts_df, how='inner', on='sku')
    branches_df.rename(columns={'id': 'product_id'}, inplace=True)
    branches_df.drop(columns='sku', inplace=True)

    session.bulk_insert_mappings(BranchProduct, branches_df.to_dict(orient='records'))
    session.commit()

    session.close()


if __name__ == "__main__":
    process_csv_files()
