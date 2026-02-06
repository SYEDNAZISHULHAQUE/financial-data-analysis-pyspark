from pyspark.sql import SparkSession
import getpass 

username = getpass.getuser()

# Spark session with Hive enabled for view/table creation
spark = SparkSession.builder \
    .config('spark.ui.port', '0') \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .config('spark.shuffle.useOldFetchProtocol', 'true') \
    .enableHiveSupport() \
    .master('yarn') \
    .getOrCreate()

# Unified customer-loan view for exploratory and ad-hoc analysis
spark.sql("""
create or replace view poc101_financialproject.customers_loan_v as
select
    l.loan_id,
    c.member_id,
    c.emp_title,
    c.emp_length,
    c.home_ownership,
    c.annual_income,
    c.address_state,
    c.address_zipcode,
    c.address_country,
    c.grade,
    c.sub_grade,
    c.verification_status,
    c.total_high_credit_limit,
    c.application_type,
    c.join_annual_income,
    c.verification_status_joint,
    l.loan_amount,
    l.funded_amount,
    l.loan_term_years,
    l.interest_rate,
    l.monthly_installment,
    l.issue_date,
    l.loan_status,
    l.loan_purpose,
    r.total_principal_received,
    r.total_interest_received,
    r.total_late_fee_received,
    r.last_payment_date,
    r.next_payment_date,
    d.delinq_2yrs,
    d.delinq_amnt,
    d.mths_since_last_delinq,
    e.pub_rec,
    e.pub_rec_bankruptcies,
    e.inq_last_6mths
from poc101_financialproject.customers c
left join poc101_financialproject.loans l
    on c.member_id = l.member_id
left join poc101_financialproject.loans_repayments r
    on l.loan_id = r.loan_id
left join poc101_financialproject.loans_defaulters_delinq d
    on c.member_id = d.member_id
left join poc101_financialproject.loans_defaulters_detail_rec_enq e
    on c.member_id = e.member_id
""")

spark.sql("select * from poc101_financialproject.customers_loan_v")

# Materialized table for downstream analytics and reporting workloads
spark.sql("""
create table poc101_financialproject.customers_loan_t as
select
    l.loan_id,
    c.member_id,
    c.emp_title,
    c.emp_length,
    c.home_ownership,
    c.annual_income,
    c.address_state,
    c.address_zipcode,
    c.address_country,
    c.grade,
    c.sub_grade,
    c.verification_status,
    c.total_high_credit_limit,
    c.application_type,
    c.join_annual_income,
    c.verification_status_joint,
    l.loan_amount,
    l.funded_amount,
    l.loan_term_years,
    l.interest_rate,
    l.monthly_installment,
    l.issue_date,
    l.loan_status,
    l.loan_purpose,
    r.total_principal_received,
    r.total_interest_received,
    r.total_late_fee_received,
    r.last_payment_date,
    r.next_payment_date,
    d.delinq_2yrs,
    d.delinq_amnt,
    d.mths_since_last_delinq,
    e.pub_rec,
    e.pub_rec_bankruptcies,
    e.inq_last_6mths
from poc101_financialproject.customers c
left join poc101_financialproject.loans l
    on c.member_id = l.member_id
left join poc101_financialproject.loans_repayments r
    on l.loan_id = r.loan_id
left join poc101_financialproject.loans_defaulters_delinq d
    on c.member_id = d.member_id
left join poc101_financialproject.loans_defaulters_detail_rec_enq e
    on c.member_id = e.member_id
""")

spark.sql("select * from poc101_financialproject.customers_loan_t")
