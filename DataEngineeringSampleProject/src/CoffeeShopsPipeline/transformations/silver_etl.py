from pyspark import pipelines as dp
import pyspark.sql.functions as F


# Please edit the sample below

@dp.table(
    name="business.coffee_shops.dim_customers",
    comment="Creating a dim table called customers"
)
@dp.expect_or_drop("customer id cannot not be null", "customerID IS NOT NULL")
@dp.expect_or_drop(
    "customer id is a foreign key in fact_sales_transactions table",
    "customerID IN(SELECT customerID from business.coffee_shops.fact_sales_transactions)"
)
def create_customer_table():
    customer_table_cols = [
        "customerID", "first_name", "last_name", "email_address", "address",
        "phone_number", "customer_city", "customer_state", "customer_country",
        "customer_continent", "customer_zipcode", "gender"
    ]
    customer_cols_select = spark.read.table("business.coffee_shops.raw_data_table").select(*customer_table_cols)
    customer_cols_select_distinct = customer_cols_select.dropDuplicates().dropna()
    return customer_cols_select_distinct

@dp.table(
    name="business.coffee_shops.dim_suppliers",
    comment="Creating a dim table called suppliers"
)
@dp.expect_or_drop("supplier id cannot be null", "supplierID IS NOT NULL")
@dp.expect_or_drop(
    "supplier id is a foreign key in fact_sales_transactions table",
    "supplierID IN(SELECT supplierID FROM business.coffee_shops.fact_sales_transactions)"
)
@dp.expect_or_drop(
    "supplier id is a foreign key in dim_franchise table",
    "supplierID IN(SELECT supplierID FROM business.coffee_shops.dim_franchises)"
)
def create_supplier_table():
    supplier_table_cols = [
        "supplierID", "supplier_name", "ingredient", "approved", "supplier_city",
        "supplier_district", "supplier_size", "supplier_continent",
        "supplier_longitude", "supplier_latitude"
    ]
    supplier_cols_select = spark.read.table("business.coffee_shops.raw_data_table").select(*supplier_table_cols)
    supplier_cols_select_distinct = supplier_cols_select.dropDuplicates().dropna()
    add_new_columns = (
        supplier_cols_select_distinct
        .withColumn(
            "ingredient_type",
            F.when(F.col("ingredient").like("%nuts%"), "nuts").otherwise("other")
        )
        .withColumn(
            "supplier_size",
            F.when(F.col("supplier_size") == "L", "Large")
            .when(F.col("supplier_size") == "M", "Medium")
            .when(F.col("supplier_size") == "S", "Small")
            .when(F.col("supplier_size") == "XL", "Extra Large")
            .otherwise("ExtraExtraLarge")
        )
    )
    return add_new_columns

@dp.table(
    name="business.coffee_shops.dim_franchises",
    comment="Creating a dim table called franchises"
)
@dp.expect_or_drop("franchise id cannot be null", "franchiseID IS NOT NULL")
@dp.expect_or_drop("supplier id cannot be null", "supplierID IS NOT NULL")
@dp.expect_or_drop(
    "franchise id is a foreign key in fact_sales_transactions table",
    "franchiseID IN(SELECT franchiseID from business.coffee_shops.fact_sales_transactions)"
)
def create_franchise_table():
    franchise_table_cols = [
        "franchiseID", "supplierID", "franchise_name", "franchise_city",
        "franchise_size", "franchise_district", "franchise_zipcode",
        "franchise_country", "franchise_longitude", "franchise_latitude"
    ]
    franchise_cols_select = spark.read.table("business.coffee_shops.raw_data_table").select(*franchise_table_cols)
    franchise_cols_select_distinct = franchise_cols_select.dropDuplicates().dropna()
    return franchise_cols_select_distinct

@dp.table(
    name="business.coffee_shops.fact_sales_transactions",
    comment="Creating a fact table called sales_transactions"
)
@dp.expect_or_drop("transaction id cannot be null", "transactionID IS NOT NULL")
def create_sales_transactions_table():
    franchise_table_cols = [
        "transactionID", "franchiseID", "customerID", "dateTime", "product",
        "quantity", "unitPrice", "totalPrice", "paymentMethod", "cardNumber"
    ]
    franchise_cols_select = spark.read.table("business.coffee_shops.raw_data_table").select(*franchise_table_cols)
    return franchise_cols_select