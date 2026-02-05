
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

spark.sql("""select member_id, count(*) as total 
from poc101_financialproject.customers
group by member_id order by total desc
""")

spark.sql("""select * from poc101_financialproject.customers 
where member_id like 'e4c167053d5418230%'""")

spark.sql("""select member_id, count(*) as total 
from poc101_financialproject.loans_defaulters_delinq
group by member_id order by total desc
""")

spark.sql("""select * from poc101_financialproject.loans_defaulters_delinq
where member_id like 'e4c167053d5418230%'""")

spark.sql("""select member_id, count(*) as total 
from poc101_financialproject.loans_defaulters_detail_rec_enq
group by member_id order by total desc
""")

spark.sql("""select * from poc101_financialproject.loans_defaulters_detail_rec_enq
where member_id like 'e4c167053d5418230%'""")

bad_data_customer_df = spark.sql("""select member_id from(select member_id, count(*)
as total from poc101_financialproject.customers
group by member_id having total > 1)""")

bad_data_customer_df.count()

bad_data_customer_df

bad_data_loans_defaulters_delinq_df = spark.sql("""select member_id from(select member_id, count(*)
as total from poc101_financialproject.loans_defaulters_delinq
group by member_id having total > 1)""")

bad_data_loans_defaulters_delinq_df.count()

bad_data_loans_defaulters_delinq_df

bad_data_loans_defaulters_detail_rec_enq_df = spark.sql("""select member_id from(select member_id, count(*)
as total from poc101_financialproject.loans_defaulters_detail_rec_enq
group by member_id having total > 1)""")

bad_data_loans_defaulters_detail_rec_enq_df.count()

bad_data_loans_defaulters_detail_rec_enq_df

bad_data_customer_df.repartition(1).write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/bad/bad_data_customers") \
.save()

bad_data_loans_defaulters_delinq_df.repartition(1).write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/bad/bad_data_loans_defaulters_delinq") \
.save()

bad_data_loans_defaulters_detail_rec_enq_df.repartition(1).write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/bad/bad_data_loans_defaulters_detail_rec_enq") \
.save()

bad_customer_data_df = bad_data_customer_df.select("member_id") \
.union(bad_data_loans_defaulters_delinq_df.select("member_id")) \
.union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))

bad_customer_data_final_df = bad_customer_data_df.distinct()

bad_customer_data_final_df.count()

bad_customer_data_final_df.repartition(1).write \
.format("csv") \
.option("header", True) \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/bad/bad_customer_data_final") \
.save()

bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

customers_df = spark.sql("""select * from poc101_financialproject.customers
where member_id NOT IN (select member_id from bad_data_customer)
""")

customers_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned_new/customers_parquet") \
.save()

loans_defaulters_delinq_df = spark.sql("""select * from poc101_financialproject.loans_defaulters_delinq
where member_id NOT IN (select member_id from bad_data_customer)
""")

loans_defaulters_delinq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned_new/loans_defaulters_delinq_parquet") \
.save()

loans_defaulters_detail_rec_enq_df = spark.sql("""select * from poc101_financialproject.loans_defaulters_detail_rec_enq
where member_id NOT IN (select member_id from bad_data_customer)
""")

loans_defaulters_detail_rec_enq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned_new/loans_defaulters_detail_rec_enq_parquet") \
.save()

spark.sql("""
create EXTERNAL TABLE poc101_financialproject.customers_new(member_id string, emp_title string, emp_length int, home_ownership string, 
annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
sub_grade string, verification_status string, total_high_credit_limit float, application_type string, 
join_annual_income float, verification_status_joint string, ingest_date timestamp)
stored as parquet
LOCATION '/public/project101/financialproject/cleaned_new/customer_parquet'
""")

spark.sql("""
create EXTERNAL TABLE poc101_financialproject.loans_defaulters_delinq_new(member_id string,delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
stored as parquet
LOCATION '/public/project101/financialproject/cleaned_new/loan_defaulters_delinq_parquet'
""")

spark.sql("""
create EXTERNAL TABLE poc101_financialproject.loans_defaulters_detail_rec_enq_new(member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
stored as parquet
LOCATION '/public/project101/financialproject/cleaned_new/loan_defaulters_detail_rec_enq_parquet'
""")

spark.sql("""select member_id, count(*) as total 
from poc101_financialproject.customers_new
group by member_id order by total desc""")



