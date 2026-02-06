from pyspark.sql import SparkSession
import getpass 

# Fetch current user to avoid hardcoding HDFS paths
username = getpass.getuser()

# Spark session setup with Hive support on YARN
spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

# Rating points used across loan score calculations
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)

# Final grade cutoffs
spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)

# Load known bad customers to exclude from scoring
bad_customer_data_final_df = spark.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.load("/public/project101/financialproject/bad/bad_customer_data_final")

bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# Payment history scoring based on last and total payments
ph_df = spark.sql("""
select c.member_id,
   case
   when p.last_payment_amount < (c.monthly_installment * 0.5) then ${spark.sql.very_bad_rated_pts}
   when p.last_payment_amount >= (c.monthly_installment * 0.5) 
        and p.last_payment_amount < c.monthly_installment then ${spark.sql.very_bad_rated_pts}
   when p.last_payment_amount = c.monthly_installment then ${spark.sql.good_rated_pts}
   when p.last_payment_amount > c.monthly_installment 
        and p.last_payment_amount <= (c.monthly_installment * 1.50) then ${spark.sql.very_good_rated_pts}
   when p.last_payment_amount > (c.monthly_installment * 1.50) then ${spark.sql.excellent_rated_pts}
   else ${spark.sql.unacceptable_rated_pts}
   end as last_payment_pts,
   case
   when p.total_payment_received >= (c.funded_amount * 0.50) then ${spark.sql.very_good_rated_pts}
   when p.total_payment_received < (c.funded_amount * 0.50) 
        and p.total_payment_received > 0 then ${spark.sql.good_rated_pts}
   when p.total_payment_received = 0 or p.total_payment_received is null 
        then ${spark.sql.unacceptable_rated_pts}
   end as total_payment_pts
from poc101_financialproject.loans_repayments p
inner join poc101_financialproject.loans c 
on c.loan_id = p.loan_id
where member_id not in (select member_id from bad_data_customer)
""")

ph_df.createOrReplaceTempView("ph_pts")

# Combine payment history with delinquency and enquiry behaviour
ldh_ph_df = spark.sql("""
select p.*,
    case
    when d.delinq_2yrs = 0 then ${spark.sql.excellent_rated_pts}
    when d.delinq_2yrs between 1 and 2 then ${spark.sql.bad_rated_pts}
    when d.delinq_2yrs between 3 and 5 then ${spark.sql.very_bad_rated_pts}
    when d.delinq_2yrs > 5 or d.delinq_2yrs is null 
         then ${spark.sql.unacceptable_grade_pts}
    end as delinq_pts,
    case
    when l.pub_rec = 0 then ${spark.sql.excellent_rated_pts}
    when l.pub_rec between 1 and 2 then ${spark.sql.bad_rated_pts}
    when l.pub_rec between 3 and 5 then ${spark.sql.very_bad_rated_pts}
    when l.pub_rec > 5 or l.pub_rec is null then ${spark.sql.very_bad_rated_pts}
    end as public_records_pts,
    case
    when l.pub_rec_bankruptcies = 0 then ${spark.sql.excellent_rated_pts}
    when l.pub_rec_bankruptcies between 1 and 2 then ${spark.sql.bad_rated_pts}
    when l.pub_rec_bankruptcies between 3 and 5 then ${spark.sql.very_bad_rated_pts}
    when l.pub_rec_bankruptcies > 5 or l.pub_rec_bankruptcies is null 
         then ${spark.sql.very_bad_rated_pts}
    end as public_bankruptcies_pts,
    case
    when l.inq_last_6mths = 0 then ${spark.sql.excellent_rated_pts}
    when l.inq_last_6mths between 1 and 2 then ${spark.sql.bad_rated_pts}
    when l.inq_last_6mths between 3 and 5 then ${spark.sql.very_bad_rated_pts}
    when l.inq_last_6mths > 5 or l.inq_last_6mths is null 
         then ${spark.sql.unacceptable_rated_pts}
    end as enq_pts
from poc101_financialproject.loans_defaulters_detail_rec_enq_new l
inner join poc101_financialproject.loans_defaulters_delinq_new d 
on d.member_id = l.member_id
inner join ph_pts p 
on p.member_id = l.member_id
where l.member_id not in (select member_id from bad_data_customer)
""")

ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")

# Financial health scoring using loan status, assets and credit exposure
fh_ldh_ph_df = spark.sql("""
select ldef.*,
   case
   when lower(l.loan_status) like '%fully paid%' then ${spark.sql.excellent_rated_pts}
   when lower(l.loan_status) like '%current%' then ${spark.sql.good_rated_pts}
   when lower(l.loan_status) like '%in grace period%' then ${spark.sql.bad_rated_pts}
   when lower(l.loan_status) like '%late (16-30 days)%' 
        or lower(l.loan_status) like '%late (31-120 days)%' 
        then ${spark.sql.very_bad_rated_pts}
   when lower(l.loan_status) like '%charged off%' then ${spark.sql.unacceptable_rated_pts}
   else ${spark.sql.unacceptable_rated_pts}
   end as loan_status_pts,
   case
   when lower(a.home_ownership) like '%own' then ${spark.sql.excellent_rated_pts}
   when lower(a.home_ownership) like '%rent' then ${spark.sql.good_rated_pts}
   when lower(a.home_ownership) like '%mortgage' then ${spark.sql.bad_rated_pts}
   when lower(a.home_ownership) like '%any' or a.home_ownership is null 
        then ${spark.sql.very_bad_rated_pts}
   end as home_pts,
   case
   when l.funded_amount <= (a.total_high_credit_limit * 0.10) 
        then ${spark.sql.excellent_rated_pts}
   when l.funded_amount <= (a.total_high_credit_limit * 0.20) 
        then ${spark.sql.very_good_rated_pts}
   when l.funded_amount <= (a.total_high_credit_limit * 0.30) 
        then ${spark.sql.good_rated_pts}
   when l.funded_amount <= (a.total_high_credit_limit * 0.50) 
        then ${spark.sql.bad_rated_pts}
   when l.funded_amount <= (a.total_high_credit_limit * 0.70) 
        then ${spark.sql.very_bad_rated_pts}
   else ${spark.sql.unacceptable_rated_pts}
   end as credit_limit_pts,
   case
   when a.grade in ('F','G') then ${spark.sql.unacceptable_rated_pts}
   else ${spark.sql.good_rated_pts}
   end as grade_pts
from ldh_ph_pts ldef
inner join poc101_financialproject.loans l 
on ldef.member_id = l.member_id
inner join poc101_financialproject.customers_new a 
on a.member_id = ldef.member_id
where ldef.member_id not in (select member_id from bad_data_customer)
""")

fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

# Weighted aggregation of all scoring dimensions
loan_score = spark.sql("""
select member_id,
((last_payment_pts + total_payment_pts) * 0.20) as payment_history_pts,
((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) 
    as defaulters_history_pts,
((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) 
    as financial_health_pts
from fh_ldh_ph_pts
""")

# Final numerical loan score
final_loan_score = loan_score.withColumn(
    'loan_score',
    loan_score.payment_history_pts 
    + loan_score.defaulters_history_pts 
    + loan_score.financial_health_pts
)

final_loan_score.createOrReplaceTempView("loan_score_eval")

# Assign final grade bucket
loan_score_final = spark.sql("""
select ls.*,
case
when loan_score > ${spark.sql.very_good_grade_pts} then 'A'
when loan_score > ${spark.sql.good_grade_pts} then 'B'
when loan_score > ${spark.sql.bad_grade_pts} then 'C'
when loan_score > ${spark.sql.very_bad_grade_pts} then 'D'
when loan_score > ${spark.sql.unacceptable_grade_pts} then 'E'
else 'F'
end as loan_final_grade
from loan_score_eval ls
""")

loan_score_final.createOrReplaceTempView("loan_final_table")

# Persist final results for downstream consumption
loan_score_final.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/processed/loan_score") \
.save()

spark.stop()
