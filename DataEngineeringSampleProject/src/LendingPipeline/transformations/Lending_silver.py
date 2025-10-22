from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from utilities import utils

# Tranform Bronze Table. Splitting Data into dimensions and fact


@dp.table(
    name="business.lending.silver_unique_ids",
    comment="Create Streaming Table for Creating Dimension Tables' Primary Keys. A silver table",
)
def create_primary_keys():
    bronze_raw_data = spark.readStream.table("business.lending.bronze_streaming_table")
    creating_unique_loan_id = bronze_raw_data.withColumn(
        "loan_id", utils.create_unique_uuid()
    )

    creating_unique_employee_id = creating_unique_loan_id.withColumn(
        "emp_id", utils.create_unique_uuid()
    )

    creating_unique_credit_id = creating_unique_employee_id.withColumn(
        "credit_id", utils.create_unique_uuid()
    )

    creating_unique_date_id = creating_unique_credit_id.withColumn(
        "date_id", utils.create_unique_uuid()
    )

    return creating_unique_date_id


@dp.table(
    name="business.lending.silver_dim_loans",
    comment="Create Loans Dimension Streaming Table. A silver table",
)
def create_dim_loans_silver_table():
    loan_dimension = spark.readStream.table(
        "business.lending.silver_unique_ids"
    ).select(
        "loan_id",
        "grade",
        "sub_grade",
        "term",
        "loan_status",
        "purpose",
        "title",
        "delinq_2yrs",
        "application_type",
    )
    loan_dimension_columns = loan_dimension.columns
    drop_loan_status = [
        col for col in loan_dimension_columns if col not in ["loan_status", "loan_id"]
    ]
    loan_dimension_dict = utils.get_new_column(drop_loan_status, "loan")

    loan_dimension_with_new_columns = loan_dimension.withColumnsRenamed(
        loan_dimension_dict
    )

    loan_delinq_class = (
        loan_dimension_with_new_columns.withColumn(
            "loan_delinq_2yrs", F.col("loan_delinq_2yrs").cast(IntegerType())
        )
        .withColumn(
            "loan_delinq_2yrs",
            F.when(F.col("loan_delinq_2yrs") <= 2, F.lit("Yes")).otherwise(F.lit("No")),
        )
        .dropDuplicates()
    )

    return loan_delinq_class


@dp.table(
    name="business.lending.silver_dim_employees",
    comment="Create Employees Dimension Streaming Table. A silver table",
)
def create_dim_employees_silver_table():
    employee_dimension = spark.readStream.table(
        "business.lending.silver_unique_ids"
    ).select(
        "emp_id",
        "emp_title",
        "emp_length",
        "home_ownership",
        "annual_inc",
        "verification_status",
        "pymnt_plan",
        "zip_code",
        "addr_state",
        "dti",
    )
    employee_dimension_columns = employee_dimension.columns
    drop_employee_title_and_length = [
        col
        for col in employee_dimension_columns
        if col not in ["emp_title", "emp_length", "emp_id"]
    ]
    employee_dimension_dict = utils.get_new_column(
        drop_employee_title_and_length, "emp"
    )
    employee_dimension_with_new_columns = employee_dimension.withColumnsRenamed(
        employee_dimension_dict
    )

    payment_plan = employee_dimension_with_new_columns.withColumn(
        "emp_pymnt_plan",
        F.when(F.col("emp_pymnt_plan") == "y", F.lit("Yes")).otherwise(F.lit("No")),
    )

    dti_column_values_update = payment_plan.withColumn(
        "emp_dti",
        F.when(F.col("emp_dti") <= 20, F.lit("Good"))
        .when(F.col("emp_dti").between(21, 40), F.lit("Fair"))
        .otherwise(F.lit("Bad")),
    )

    annual_income_values_update = (
        dti_column_values_update.withColumn(
            "emp_annual_inc", F.col("emp_annual_inc").cast(IntegerType())
        )
        .withColumn(
            "emp_annual_inc",
            F.when(F.col("emp_annual_inc") <= 30000, F.lit("Very Low Income"))
            .when(F.col("emp_annual_inc").between(30001, 50000), F.lit("Low Income"))
            .when(F.col("emp_annual_inc").between(50001, 70000), F.lit("Fair Income"))
            .when(F.col("emp_annual_inc").between(70001, 100000), F.lit("High Income"))
            .otherwise(F.lit("Very High Income")),
        )
        .dropDuplicates()
    )

    return annual_income_values_update


@dp.table(
    name="business.lending.silver_dim_credit",
    comment="Create Credit Dimension Streaming Table. A silver table",
)
def create_dim_credit_silver_table():
    credit_dimension = spark.readStream.table(
        "business.lending.silver_unique_ids"
    ).select(
        "credit_id",
        "inq_last_6mths",
        "open_acc",
        "pub_rec",
        "inq_last_12m",
        "acc_open_past_24mths",
        "revol_bal",
        "revol_util",
        "total_acc",
        "open_acc_6m",
    )
    credit_dimension_columns = credit_dimension.columns
    drop_credit_id = [col for col in credit_dimension_columns if col != "credit_id"]
    credit_dimension_dict = utils.get_new_column(drop_credit_id, "credit")
    credit_dimension_with_new_columns = credit_dimension.withColumnsRenamed(
        credit_dimension_dict
    )

    inq_last_six_months_column_values_update = (
        credit_dimension_with_new_columns.withColumn(
            "credit_inq_last_6mths",
            F.when(
                F.col("credit_inq_last_6mths") <= 4, F.col("credit_inq_last_6mths")
            ).otherwise(F.lit(">4")),
        )
    )

    open_accounts_column_values_update = (
        inq_last_six_months_column_values_update.withColumn(
            "credit_open_acc",
            F.when(F.col("credit_open_acc") <= 5, F.lit("Healthy"))
            .when(F.col("credit_open_acc").between(6, 12), F.lit("Unhealthy"))
            .when(F.col("credit_open_acc").between(12, 20), F.lit("Very Unhealthy"))
            .otherwise(F.lit("Excessive & Concerning")),
        )
    )

    credit_pub_rec_column_values_update = open_accounts_column_values_update.withColumn(
        "credit_pub_rec",
        F.when(F.col("credit_pub_rec") <= 4, F.col("credit_pub_rec")).otherwise(
            F.lit(">4")
        ),
    )

    inq_last_twelve_months_column_values_update = (
        credit_pub_rec_column_values_update.withColumn(
            "credit_inq_last_12m",
            F.when(F.col("credit_inq_last_12m") <= 4, F.lit("Healthy"))
            .when(F.col("credit_inq_last_12m") <= 4, F.lit("Concerning"))
            .otherwise(F.lit("Unhealthy")),
        )
    )

    open_accounts_past_24_months_column_values_update = (
        inq_last_twelve_months_column_values_update.withColumn(
            "credit_acc_open_past_24mths",
            F.when(F.col("credit_acc_open_past_24mths") <= 5, F.lit("Healthy"))
            .when(
                F.col("credit_acc_open_past_24mths").between(6, 12), F.lit("Unhealthy")
            )
            .when(
                F.col("credit_acc_open_past_24mths").between(12, 20),
                F.lit("Very Unhealthy"),
            )
            .otherwise(F.lit("Excessive & Concerning")),
        )
    )

    revol_bal_column_values_update = (
        open_accounts_past_24_months_column_values_update.withColumn(
            "credit_revol_bal",
            F.when(F.col("credit_revol_bal") < 15000, F.lit("Healthy"))
            .when(F.col("credit_revol_bal").between(15000, 30000), F.lit("Okay"))
            .when(
                F.col("credit_revol_bal").between(30001, 45000), F.lit("Very Unhealthy")
            )
            .otherwise(F.lit("Excessive & Concerning")),
        )
    )

    open_accounts_total_column_values_update = (
        revol_bal_column_values_update.withColumn(
            "credit_total_acc", F.col("credit_total_acc").cast(IntegerType())
        ).withColumn(
            "credit_total_acc",
            F.when(F.col("credit_total_acc") <= 5, F.lit("Healthy"))
            .when(F.col("credit_total_acc").between(6, 12), F.lit("Unhealthy"))
            .when(F.col("credit_total_acc").between(12, 20), F.lit("Very Unhealthy"))
            .otherwise(F.lit("Excessive & Concerning")),
        )
    )

    open_accounts_past_six_months_column_values_update = (
        open_accounts_total_column_values_update.withColumn(
            "credit_open_acc_6m",
            F.when(
                F.col("credit_open_acc_6m") <= 3, F.col("credit_open_acc_6m")
            ).otherwise(F.lit(">3")),
        )
    )

    revol_util_column_values_update = (
        open_accounts_past_six_months_column_values_update.withColumn(
            "credit_revol_util",
            F.substring(
                F.col("credit_revol_util"), 1, F.length(F.col("credit_revol_util")) - 1
            ),
        ).withColumn(
            "credit_revol_util",
            F.when(F.col("credit_revol_util") <= 30, F.lit("Good"))
            .when(F.col("credit_revol_util").between(31, 45), F.lit("Fair"))
            .otherwise(F.lit("Bad")),
        )
    ).dropDuplicates()

    return revol_util_column_values_update


@dp.table(
    name="business.lending.silver_dim_time",
    comment="Create Time Dimension Streaming Table. A silver table",
)
def create_dim_time_silver_table():
    time_dimension = spark.readStream.table(
        "business.lending.silver_unique_ids"
    ).select(
        "date_id",
        "last_pymnt_d",
        "next_pymnt_d",
        "last_credit_pull_d",
        "earliest_cr_line",
    )
    time_dimension_columns = time_dimension.columns
    drop_date_id = [col for col in time_dimension_columns if col != "date_id"]
    time_dimension_dict = utils.get_new_column(drop_date_id, "date")
    time_dimension_with_new_columns = time_dimension.withColumnsRenamed(
        time_dimension_dict
    )

    convert_date_string_to_date = (
        time_dimension_with_new_columns.withColumn(
            "date_last_pymnt_d", F.to_date(F.col("date_last_pymnt_d"), "MMM-yyyy")
        )
        .withColumn(
            "date_next_pymnt_d", F.to_date(F.col("date_next_pymnt_d"), "MMM-yyyy")
        )
        .withColumn(
            "date_last_credit_pull_d",
            F.to_date(F.col("date_last_credit_pull_d"), "MMM-yyyy"),
        )
        .withColumn(
            "date_earliest_cr_line",
            F.to_date(F.col("date_earliest_cr_line"), "MMM-yyyy"),
        )
    ).dropDuplicates()

    return convert_date_string_to_date


@dp.table(
    name="business.lending.silver_fact_loan_activity",
    comment="Create Loan Activity Fact Streaming Table. A silver table",
)
def create_fact_loan_activity_silver_table():
    fact_loan_activity = spark.readStream.table(
        "business.lending.silver_unique_ids"
    ).select(
        F.col("annual_inc").alias("emp_annual_inc_amnt"),
        "loan_id",
        "emp_id",
        "credit_id",
        "date_id",
        "loan_amnt",
        "funded_amnt",
        "funded_amnt_inv",
        "int_rate",
        "out_prncp",
        "out_prncp_inv",
        "total_pymnt",
        "total_pymnt_inv",
        "last_pymnt_amnt",
    )

    int_rate_values_update = (
        fact_loan_activity.withColumn(
            "int_rate", F.substring(F.col("int_rate"),1,F.length(F.col("int_rate"))-F.lit(1))
        )
        .withColumn("int_rate", F.regexp_replace(F.col("int_rate"), "f", ""))
        .withColumn("int_rate", F.regexp_replace(F.col("int_rate"), "n/a", ""))
        .withColumn("int_rate", F.col("int_rate").cast(DoubleType()))
        .withColumn("out_prncp", F.regexp_replace(F.col("out_prncp"), "%", ""))
        .withColumn("out_prncp", F.regexp_replace(F.col("out_prncp"), "f", ""))
        .withColumn("out_prncp", F.regexp_replace(F.col("out_prncp"), "n/a", ""))
        .withColumn("out_prncp", F.col("out_prncp").cast(DoubleType()))
        .withColumn("total_pymnt", F.regexp_replace(F.col("total_pymnt"), "%", ""))
        .withColumn("total_pymnt", F.regexp_replace(F.col("total_pymnt"), "f", ""))
        .withColumn("total_pymnt", F.regexp_replace(F.col("total_pymnt"), "n/a", ""))
        .withColumn("total_pymnt", F.col("total_pymnt").cast(DoubleType()))
        .withColumn(
            "last_pymnt_amnt", F.regexp_replace(F.col("last_pymnt_amnt"), "%", "")
        )
        .withColumn(
            "last_pymnt_amnt", F.regexp_replace(F.col("last_pymnt_amnt"), "f", "")
        )
        .withColumn(
            "last_pymnt_amnt", F.regexp_replace(F.col("last_pymnt_amnt"), "n/a", "")
        )
        .withColumn("last_pymnt_amnt", F.col("last_pymnt_amnt").cast(DoubleType()))
    )

    creating_unique_loan_activity_id = int_rate_values_update.withColumn(
        "loan_activity_id", utils.create_unique_uuid()
    ).select(
        "loan_activity_id",
        "loan_id",
        "emp_id",
        "credit_id",
        "date_id",
        "loan_amnt",
        "funded_amnt",
        "funded_amnt_inv",
        "int_rate",
        "out_prncp",
        "out_prncp_inv",
        "total_pymnt",
        "total_pymnt_inv",
        "last_pymnt_amnt",
        "emp_annual_inc_amnt"
    )

    return creating_unique_loan_activity_id
