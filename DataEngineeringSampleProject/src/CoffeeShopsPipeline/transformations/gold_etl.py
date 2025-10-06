from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="business.coffee_shops.supplier_franchise_proximity",
    comment="a table that shows which suppliers are the closest to the franchises they do business with",
)
def create_distance_analysis_table():
    km_to_miles_factor = 0.621371
    suppliers_table = spark.table("business.coffee_shops.dim_suppliers")
    franshises_table = spark.table("business.coffee_shops.dim_franchises")
    join_franchise_supplier_tables = (
        franshises_table.join(suppliers_table, ["supplierID"], "inner")
        .withColumn(
            "DistanceMiles",
            (
                F.round(
                    (
                        F.acos(
                            (
                                F.sin(F.radians(F.col("franchise_latitude")))
                                * F.sin(F.radians(F.col("supplier_latitude")))
                            )
                            + (
                                (
                                    F.cos(F.radians(F.col("franchise_latitude")))
                                    * F.cos(F.radians(F.col("supplier_latitude")))
                                )
                                * (
                                    F.cos(
                                        F.radians("franchise_longitude")
                                        - F.radians("supplier_longitude")
                                    )
                                )
                            )
                        )
                        * F.lit(6371.0)
                    ),
                    4,
                )
            )
            * F.lit(km_to_miles_factor),
        )
        .withColumn(
            "InSameCountry",
            F.when(F.col("supplier_city") == F.col("franchise_city"), True).otherwise(
                False
            ),
        )
        .withColumn(
            "InSameDistrict",
            F.when(
                F.col("supplier_district") == F.col("franchise_district"), True
            ).otherwise(False),
        )
    )

    return join_franchise_supplier_tables


@dp.table(
    name="business.coffee_shops.customer_class",
    comment="Classify which customers have the largest purchases",
)
def create_customer_class_table():
    sales_transactions = spark.table("business.coffee_shops.fact_sales_transactions")
    customers = spark.table("business.coffee_shops.dim_customers").withColumn(
        "FullName", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
    )
    join_customers_and_sales_transactions = sales_transactions.join(
        customers, ["customerID"], "left"
    )
    customer_class = join_customers_and_sales_transactions.groupBy("QuantityClass").agg(
        F.sum("totalPrice").alias("totalPurchases"),
        F.count("quantity").alias("totalQuantity"),
        F.collect_set("customerID").alias("customerIDs"),
        F.collect_set("FullName").alias("CustomerNames"),
    )

    return customer_class
