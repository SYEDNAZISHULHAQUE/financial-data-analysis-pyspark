
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

# #### 1. create a dataframe with proper datatypes 

customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'

customers_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(customer_schema) \
.load("/public/project101/financialproject/raw/customers_data_csv")

customers_raw_df

customers_raw_df.printSchema()

# #### 2. Rename a few columns

customer_df_renamed = customers_raw_df.withColumnRenamed("annual_inc", "annual_income") \
.withColumnRenamed("addr_state", "address_state") \
.withColumnRenamed("zip_code", "address_zipcode") \
.withColumnRenamed("country", "address_country") \
.withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint", "join_annual_income")

customer_df_renamed

from pyspark.sql.functions import current_timestamp

# #### 3. insert a new column named as ingestion date(current time)

customers_df_ingestd = customer_df_renamed.withColumn("ingest_date", current_timestamp())

customers_df_ingestd

# #### 4. Remove complete duplicate rows

customers_df_ingestd.count()

customers_distinct = customers_df_ingestd.distinct()

customers_distinct.count()

customers_distinct.createOrReplaceTempView("customers")

spark.sql("select * from customers")

# #### 5. Remove the rows where annual_income is null

spark.sql("select count(*) from customers where annual_income is null")

customers_income_filtered = spark.sql("select * from customers where annual_income is not null")

customers_income_filtered.createOrReplaceTempView("customers")

spark.sql("select count(*) from customers where annual_income is null")

# ### 6. convert emp_length to integer

spark.sql("select distinct(emp_length) from customers")

from pyspark.sql.functions import regexp_replace, col

customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)",""))

customers_emplength_cleaned

customers_emplength_cleaned.printSchema()

customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))

customers_emplength_casted

customers_emplength_casted.printSchema()

# #### 7. we need to replace all the nulls in emp_length column with average of this column

customers_emplength_casted.filter("emp_length is null").count()

customers_emplength_casted.createOrReplaceTempView("customers")

avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()

print(avg_emp_length)

avg_emp_duration = avg_emp_length[0][0]

print(avg_emp_duration)

customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])

customers_emplength_replaced

customers_emplength_replaced.filter("emp_length is null").count()

# #### 8. Clean the address_state(it should be 2 characters only),replace all others with NA

customers_emplength_replaced.createOrReplaceTempView("customers")

spark.sql("select distinct(address_state) from customers")

spark.sql("select count(address_state) from customers where length(address_state)>2")

from pyspark.sql.functions import when, col, length

customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state"))> 2, "NA").otherwise(col("address_state"))
)

customers_state_cleaned

customers_state_cleaned.select("address_state").distinct()

customers_state_cleaned.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/customers_parquet") \
.save()

customers_state_cleaned.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/customers_csv") \
.save()