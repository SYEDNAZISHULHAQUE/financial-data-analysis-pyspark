
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

loans_repay_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.option("inferSchema", True) \
.load("/public/project101/financialproject/raw/loans_repayments_csv")

loans_repay_raw_df

loans_repay_raw_df.printSchema()

loans_repay_schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'

loans_repay_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loans_repay_schema) \
.load("/public/project101/financialproject/raw/loans_repayments_csv")

loans_repay_raw_df.printSchema()

from pyspark.sql.functions import current_timestamp

loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date", current_timestamp())

loans_repay_df_ingestd

loans_repay_df_ingestd.printSchema()

loans_repay_df_ingestd.count()

loans_repay_df_ingestd.createOrReplaceTempView("loan_repayments")

spark.sql("select count(*) from loan_repayments where total_principal_received is null")

columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]

loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)

loans_repay_filtered_df.count()

loans_repay_filtered_df.createOrReplaceTempView("loan_repayments")

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0")

spark.sql("select count(*) from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0")

spark.sql("select * from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0")

from pyspark.sql.functions import when, col

loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
   "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
)

loans_payments_fixed_df

loans_payments_fixed_df.filter("loan_id == '1064185'")

loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")

loans_payments_fixed2_df.filter("last_payment_date = 0.0").count()

loans_payments_fixed2_df.filter("next_payment_date = 0.0").count()

loans_payments_fixed2_df.filter("last_payment_date is null").count()

loans_payments_fixed2_df.filter("next_payment_date is null").count()

loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
  "last_payment_date",
   when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
)

loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
  "last_payment_date",
   when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
)

loans_payments_ndate_fixed_df.filter("last_payment_date = 0.0").count()

loans_payments_ndate_fixed_df.filter("next_payment_date = 0.0").count()

loans_payments_ndate_fixed_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_repayments_parquet") \
.save()

loans_payments_ndate_fixed_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_repayments_csv") \
.save()



