import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job= Job(glueContext)
job.init(args['JOB_NAME'], args)

Df1 = glueContext.create_dynamic_frame.from_catalog(
    database="incremental-load-check-database",
    table_name="employee_employees_csv"
    transformation_ctx = "s3_input_new"     ## glue will remember for this transformation context what it has processed previously. kind of check pointing or state maintenance technique.
    )
 value_df = Df1.select(from_json(col("value").cast("string"), schema).alias("value"))

    notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
        .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    #target_df = notification_df.selectExpr("InvoiceNumber as key", "to_json(struct(*)) as value")

    target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                 """to_json(named_struct(
                                                 'CustomerCardNo', CustomerCardNo,
                                                 'TotalAmount', TotalAmount,
                                                 'EarnedLoyaltyPoints', TotalAmount * 0.2)) as value""")

data_frame = target_df.toDF()
data_frame.write.json(s3_path)
job.commit()
