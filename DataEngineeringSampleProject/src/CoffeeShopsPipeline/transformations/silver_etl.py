from pyspark import pipelines as dp
import pyspark.sql.functions as F
from utilities import utils


@dp.table(
    name="business.coffee_shops.dim_customers",
    comment="Creating a dim table called customers",
)
@dp.expect_or_drop("customer id cannot not be null", "customerID IS NOT NULL")
def create_customer_table():
    customer_table_cols = [
        "customerID",
        "first_name",
        "last_name",
        "email_address",
        "address",
        "phone_number",
        "customer_city",
        "customer_state",
        "customer_country",
        "customer_continent",
        "customer_zipcode",
        "gender",
    ]
    customer_cols_select = spark.read.table(
        "business.coffee_shops.raw_data_streaming_table"
    ).select(*customer_table_cols)
    customer_cols_select_distinct = customer_cols_select.dropDuplicates().dropna()
    gender_condition_statement = F.when(F.col("gender").isin(["male","Male"]), "M").otherwise("F")
    gender_change = customer_cols_select_distinct.withColumn("gender", gender_condition_statement)
    return gender_change


@dp.table(
    name="business.coffee_shops.dim_suppliers",
    comment="Creating a dim table called suppliers",
)
@dp.expect_or_drop("supplier id cannot be null", "supplierID IS NOT NULL")

def create_supplier_table():
    supplier_table_cols = [
        "supplierID",
        "supplier_name",
        "ingredient",
        "approved",
        "supplier_city",
        "supplier_district",
        "supplier_size",
        "supplier_continent",
        "supplier_longitude",
        "supplier_latitude",
    ]
    supplier_cols_select = spark.read.table(
        "business.coffee_shops.raw_data_streaming_table"
    ).select(*supplier_table_cols)
    supplier_cols_select_distinct = supplier_cols_select.dropDuplicates().dropna()
    ingredient_type_condition_statement = F.when(
        F.col("ingredient").like("%nuts%"), "nuts"
    ).otherwise("other")
    supplier_size_condition_statement = (
        F.when(F.col("supplier_size") == "L", "Large")
        .when(F.col("supplier_size") == "M", "Medium")
        .when(F.col("supplier_size") == "S", "Small")
        .when(F.col("supplier_size") == "XL", "Extra Large")
        .otherwise("ExtraExtraLarge")
    )

    approved_condition_statement= F.when(F.col("approved")=="No","N").when(F.col("approved")=="Yes","Y").otherwise(F.col("approved"))

    add_new_columns = supplier_cols_select_distinct.withColumn(
        "ingredient_type", ingredient_type_condition_statement
    ).withColumn("supplier_size", supplier_size_condition_statement).withColumn("approved",approved_condition_statement)

    return add_new_columns


@dp.table(
    name="business.coffee_shops.dim_franchises",
    comment="Creating a dim table called franchises",
)
@dp.expect_or_drop("franchise id cannot be null", "franchiseID IS NOT NULL")
@dp.expect_or_drop("supplier id cannot be null", "supplierID IS NOT NULL")
def create_franchise_table():
    franchise_table_cols = [
        "franchiseID",
        "supplierID",
        "franchise_name",
        "franchise_city",
        "franchise_size",
        "franchise_district",
        "franchise_zipcode",
        "franchise_country",
        "franchise_longitude",
        "franchise_latitude",
    ]

    franchise_cols_select = spark.read.table(
        "business.coffee_shops.raw_data_streaming_table"
    ).select(*franchise_table_cols)
    franchise_cols_select_distinct = franchise_cols_select.dropDuplicates().dropna()

    return franchise_cols_select_distinct


@dp.table(
    name="business.coffee_shops.fact_sales_transactions",
    comment="Creating a fact table called sales_transactions",
)
@dp.expect_or_drop("transaction id cannot be null", "transactionID IS NOT NULL")
@dp.expect_or_drop("quanity should be >0", "quantity >0")
@dp.expect_or_drop("total price should be >0", "totalPrice >0")
@dp.expect_or_drop("unit price should be >0", "unitPrice >0")
def create_sales_transactions_table():
    sales_transactions_table_cols = [
        "transactionID",
        "franchiseID",
        "customerID",
        "dateTime",
        "product",
        "quantity",
        "unitPrice",
        "totalPrice",
        "paymentMethod",
        "cardNumber",
    ]
    sales_transactions_table_select = spark.read.table(
        "business.coffee_shops.raw_data_streaming_table"
    ).select(*sales_transactions_table_cols)

    classify_quantity_condition = (
        F.when(F.col("quantity").between(1, 25), "SmallPurchase")
        .when(F.col("quantity").between(26, 45), "MediumPurchase")
        .otherwise("BigPurchase")
    )
    classify_quanity = sales_transactions_table_select.withColumn(
        "QuantityClass", classify_quantity_condition
    )

    return classify_quanity
