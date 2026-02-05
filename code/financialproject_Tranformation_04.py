
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

loans_def_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.option("inferSchema", True) \
.load("/public/project101/financialproject/raw/loans_defaulters_csv")

loans_def_raw_df

loans_def_raw_df.printSchema()

loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

spark.sql("select distinct(delinq_2yrs) from loan_defaulters")

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"

loans_def_raw_df = spark.read \
.format("csv") \
.option("header",True) \
.schema(loan_defaulters_schema) \
.load("/public/project101/financialproject/raw/loans_defaulters_csv")

loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

from pyspark.sql.functions import col

loans_def_processed_df = loans_def_raw_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

loans_def_processed_df.createOrReplaceTempView("loan_defaulters")

spark.sql("select count(*) from loan_defaulters where delinq_2yrs is null")

spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

loans_def_delinq_df = spark.sql("select member_id,delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) from loan_defaulters where delinq_2yrs > 0 or mths_since_last_delinq > 0")

loans_def_delinq_df

loans_def_delinq_df.count()

loans_def_records_enq_df = spark.sql("select member_id from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")

loans_def_records_enq_df

loans_def_records_enq_df.count()

loans_def_delinq_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_defaulters_deling_csv") \
.save()

loans_def_delinq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_defaulters_deling_parquet") \
.save()

loans_def_records_enq_df.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_defaulters_records_enq_csv") \
.save()

loans_def_records_enq_df.write \
.format("parquet") \
.mode("overwrite") \
.option("path", "/user/poc101/financialproject/raw/cleaned/loans_defaulters_records_enq_parquet") \
.save()