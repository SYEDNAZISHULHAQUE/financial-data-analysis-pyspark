
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

# #### 1. create a dataframe with proper datatypes and names

loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'

loans_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loans_schema) \
.load("/public/project101/financialproject/raw/loans_data_csv")

loans_raw_df

loans_raw_df.printSchema()

from pyspark.sql.functions import current_timestamp

# #### 2. insert a new column named as ingestion date(current time)

loans_df_ingestd = loans_raw_df.withColumn("ingest_date", current_timestamp())

loans_df_ingestd

loans_df_ingestd.createOrReplaceTempView("loans")

spark.sql("select count(*) from loans")

spark.sql("select * from loans where loan_amount is null")

# #### 3. Dropping the rows which has null values in the mentioned columns

columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]

loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)

loans_filtered_df.count()

loans_filtered_df.createOrReplaceTempView("loans")

loans_filtered_df

# #### 4. convert loan_term_months to integer

from pyspark.sql.functions import regexp_replace, col

loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
.cast("int") / 12) \
.cast("int")) \
.withColumnRenamed("loan_term_months","loan_term_years")

loans_term_modified_df

loans_term_modified_df.printSchema()

# #### 5. Clean the loans_purpose column

loans_term_modified_df.createOrReplaceTempView("loans")

spark.sql("select distinct(loan_purpose) from loans")

spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]

from pyspark.sql.functions import when

loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))

loans_purpose_modified.createOrReplaceTempView("loans")

spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc")

from pyspark.sql.functions import count

loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())

loans_purpose_modified.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_parquet") \
.save()

loans_purpose_modified.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_csv") \
.save()