from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="business.lending.gold_employee_loans", comment="Gold Table for Employee Loans"
)
def create_employee_loans_table():
    silver_loan_activity_table = spark.readStream.table(
        "business.lending.silver_fact_loan_activity"
    )
    silver_employees_table = spark.readStream.table(
        "business.lending.silver_dim_employees"
    )
    silver_loans_table = spark.readStream.table("business.lending.silver_dim_loans")
    employee_join_to_loan_activity = silver_employees_table.join(
        silver_loan_activity_table.select("emp_id", "loan_id"), ["emp_id"]
    )
    employee_join_to_loans = employee_join_to_loan_activity.drop("emp_id").join(
        silver_loans_table, ["loan_id"]
    )

    return employee_join_to_loans


@dp.table(
    name="business.lending.gold_employee_credit_info",
    comment="Gold Table for Employee Credit Info",
)
def create_employee_credit_table():
    silver_loan_activity_table = spark.readStream.table(
        "business.lending.silver_fact_loan_activity"
    )
    silver_employees_table = spark.readStream.table(
        "business.lending.silver_dim_employees"
    )
    silver_credit_table = spark.readStream.table("business.lending.silver_dim_credit")
    employee_join_to_loan_activity = silver_employees_table.join(
        silver_loan_activity_table.select("emp_id", "credit_id"), ["emp_id"]
    )
    employee_join_to_credit = employee_join_to_loan_activity.join(
        silver_credit_table, ["credit_id"]
    )

    return employee_join_to_credit


@dp.table(
    name="business.lending.gold_loans_and_activity_info",
    comment="Gold Table for Loans and Activity",
)
def create_loan_activity_table():
    silver_loan_activity_table = spark.readStream.table(
        "business.lending.silver_fact_loan_activity"
    )
    silver_loans_table = spark.readStream.table("business.lending.silver_dim_loans")

    loans_join_to_loan_activity = silver_loans_table.join(
        silver_loan_activity_table.drop("emp_id", "credit_id", "date_id"), ["loan_id"]
    )

    return loans_join_to_loan_activity


@dp.table(
    name="business.lending.gold_employee_loan_activity",
    comment="Gold Table for Employees and Activity",
)
def create_employee_loan_activity_table():
    silver_loan_activity_table = spark.readStream.table(
        "business.lending.silver_fact_loan_activity"
    )
    silver_employees_table = spark.readStream.table(
        "business.lending.silver_dim_employees"
    )

    employees_join_to_loan_activity = silver_employees_table.join(
        silver_loan_activity_table.drop("credit_id", "date_id", "loan_id"), ["emp_id"]
    )

    return employees_join_to_loan_activity


@dp.table(
    name="business.lending.gold_employee_employee_time_and_loan_activity",
    comment="Gold Table for Employees, Time and Loan Activity",
)
def create_employee_loan_activity_table():
    silver_loan_activity_table = spark.readStream.table(
        "business.lending.silver_fact_loan_activity"
    )
    silver_time_table = spark.readStream.table("business.lending.silver_dim_time")
    silver_employees_table = spark.readStream.table(
        "business.lending.silver_dim_employees"
    )

    time_join_to_loan_activity = silver_time_table.join(
        silver_loan_activity_table.drop("credit_id", "loan_id"), ["date_id"]
    )

    employee_join_to_time_and_loan_activity = time_join_to_loan_activity.join(
        silver_employees_table.drop("credit_id", "loan_id"), ["emp_id"]
    )

    return employee_join_to_time_and_loan_activity
