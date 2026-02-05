
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

raw_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("/public/project101/datasets/accepted_2007_to_2018Q4.csv")

raw_df.createOrReplaceTempView("financial_data")

spark.sql("select * from financial_data")

from pyspark.sql.functions import sha2,concat_ws

new_df = raw_df.withColumn("name_sha2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))

new_df.createOrReplaceTempView("newtable")

spark.sql("select count(*) from newtable")

spark.sql("select count(distinct(name_sha2)) from newtable")

spark.sql("""select name_sha2, count(*) as total_cnt 
from newtable group by name_sha2 having total_cnt>1 order by total_cnt desc""")

spark.sql("select * from newtable where name_sha2 like 'e3b0c44298fc1c149%'")

spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from newtable
""").repartition(1).write \
.option("header","true")\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/customers_data_csv") \
.save()

customers_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("/user/poc101/financialproject/raw/customers_data_csv")

customers_df

spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
title from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/loans_data_csv") \
.save()

loans_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("/user/poc101/financialproject/raw/loans_data_csv")

loans_df

spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/loans_repayments_csv") \
.save()

loans_repayments_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("/user/poc101/financialproject/raw/loans_repayments_csv")

loans_repayments_df

spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/loans_defaulters_csv") \
.save()

loans_defaulters_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("/user/poc101/financialproject/raw/loans_defaulters_csv")

loans_defaulters_df

