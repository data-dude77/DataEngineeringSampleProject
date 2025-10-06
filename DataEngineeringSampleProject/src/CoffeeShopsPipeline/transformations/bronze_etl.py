from pyspark import pipelines as dp
from pyspark.sql.functions import col


# Please edit the sample below
from pyspark import pipelines as dp
import pyspark.sql.functions as F

use_catalog_sql = """
USE CATALOG business
"""

use_schema_sql = """
USE SCHEMA coffee_shops
"""

spark.sql(use_catalog_sql)
spark.sql(use_schema_sql)


# This full table is created to represent unorganized data set. No dims or fact table formed


@dp.table(
    name="business.coffee_shops.raw_data_table",
    comment="Creating a full table of suppliers,customers,franchises and sales transations",
)
def create_raw_table():

    franchise_col_rename_dict = {
        "name": "franchise_name",
        "city": "franchise_city",
        "district": "franchise_district",
        "zipcode": "franchise_zipcode",
        "country": "franchise_country",
        "size": "franchise_size",
        "longitude": "franchise_longitude",
        "latitude": "franchise_latitude",
    }

    customer_col_rename_dict = {
        "continent": "customer_continent",
        "city": "customer_city",
        "state": "customer_state",
        "postal_zip_code": "customer_zipcode",
        "country": "customer_country",
    }

    supplier_col_rename_dict = {
        "continent": "supplier_continent",
        "city": "supplier_city",
        "district": "supplier_district",
        "name": "supplier_name",
        "size": "supplier_size",
        "longitude": "supplier_longitude",
        "latitude": "supplier_latitude",
    }
    sales_transactions_col_rename_dict_one = {
        "transactionID": "customerID_new",
        "customerID": "transactionID_new",
    }
    sales_transactions_col_rename_dict_two = {
        "customerID_new": "customerID",
        "transactionID_new": "transactionID",
    }

    bakehouse_suppliers = spark.read.table(
        "samples.bakehouse.sales_suppliers"
    ).withColumnsRenamed(supplier_col_rename_dict)
    bakehouse_customers = spark.read.table(
        "samples.bakehouse.sales_customers"
    ).withColumnsRenamed(customer_col_rename_dict)
    bakehouse_sales_transactions_one = spark.read.table(
        "samples.bakehouse.sales_transactions"
    ).withColumnsRenamed(sales_transactions_col_rename_dict_one)
    bakehouse_sales_transactions_two = (
        bakehouse_sales_transactions_one.withColumnsRenamed(
            sales_transactions_col_rename_dict_two
        )
    )
    bakehouse_franchises = spark.read.table(
        "samples.bakehouse.sales_franchises"
    ).withColumnsRenamed(franchise_col_rename_dict)
    full_table = (
        bakehouse_sales_transactions_two.join(
            bakehouse_customers, ["customerID"], "left"
        )
        .join(bakehouse_franchises, ["franchiseID"], "left")
        .join(bakehouse_suppliers, ["supplierID"], "left")
    )

    return full_table
