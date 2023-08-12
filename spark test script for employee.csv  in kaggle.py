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

empDf = glueContext.create_dynamic_frame.from_catalog(
    database="incremental-load-check-database",
    table_name="employee_employees_csv"
    transformation_ctx = "s3_input_new"     ## glue will remember for this transformation context what it has processed previously. kind of check pointing or state maintenance technique.
    )

empDf.printSchema()
sparkEmpDf = empDf.toDF()
sparkEmpDf.show()
print(sparkEmpDf.count())

job.commit()
