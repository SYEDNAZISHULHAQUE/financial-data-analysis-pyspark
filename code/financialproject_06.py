
from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

customers_df = spark.read \
.format("parquet") \
.load("/public/project101/financialproject/cleaned/customers_parquet")

customers_df

spark.sql("create database poc101_financialproject")

spark.sql("""create external table poc101_financialproject.customers(
member_id string, emp_title string, emp_length int, 
home_ownership string, annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
sub_grade string, verification_status string, total_high_credit_limit float, application_type string, join_annual_income float, 
verification_status_joint string, ingest_date timestamp)
stored as parquet location  '/public/project101/financialproject/cleaned/customers_parquet'
""")

spark.sql("select * from poc101_financialproject.customers")

spark.sql("""
create external table poc101_financialproject.loans(
loan_id string, member_id string, loan_amount float, funded_amount float,
loan_term_years integer, interest_rate float, monthly_installment float, issue_date string,
loan_status string, loan_purpose string, loan_title string, ingest_date timestamp)
stored as parquet
location '/public/project101/financialproject/cleaned/loans_parquet'
""")

spark.sql("select * from poc101_financialproject.loans")

spark.sql("""CREATE EXTERNAL TABLE poc101_financialproject.loans_repayments(loan_id string, total_principal_received float,
total_interest_received float,total_late_fee_received float,total_payment_received float,last_payment_amount float,
last_payment_date string,next_payment_date string,ingest_date timestamp)
stored as parquet LOCATION '/public/project101/financialproject/cleaned/loans_repayments_parquet'
""")

spark.sql("select * from poc101_financialproject.loans_repayments")

spark.sql("""CREATE EXTERNAL TABLE poc101_financialproject.loans_defaulters_delinq(
member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
stored as parquet LOCATION '/public/project101/financialproject/cleaned/loans_defaulters_delinq_parquet'""")

spark.sql("select * from poc101_financialproject.loans_defaulters_delinq")

spark.sql("""CREATE EXTERNAL TABLE poc101_financialproject.loans_defaulters_detail_rec_enq(
member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
stored as parquet LOCATION '/public/project101/financialproject/cleaned/loans_defaulters_detail_records_enq_parquet'""")

spark.sql("select * from poc101_financialproject.loans_defaulters_detail_rec_enq")

