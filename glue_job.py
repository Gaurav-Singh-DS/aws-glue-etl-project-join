import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Dept
Dept_node1757276858302 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projectglueaw2025/Source/employee.csv"], "recurse": True}, transformation_ctx="Dept_node1757276858302")

# Script generated for node Emp
Emp_node1757276949440 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projectglueaw2025/Source/department.csv"], "recurse": True}, transformation_ctx="Emp_node1757276949440")

# Script generated for node Join
Join_node1757276999458 = Join.apply(frame1=Emp_node1757276949440, frame2=Dept_node1757276858302, keys1=["dept_id"], keys2=["dept_id"], transformation_ctx="Join_node1757276999458")

# Script generated for node Drop Fields
DropFields_node1757277125941 = DropFields.apply(frame=Join_node1757276999458, paths=["`.dept_id`", "location", "manager", "hire_date"], transformation_ctx="DropFields_node1757277125941")

#rearrange the col 
#1st conver into DF then rearrange col and then convert back to dyanamic frame 
df = DropFields_node1757277125941.toDF()
df = df.select("emp_id", "emp_name", "dept_id","dept_name","phone","email", "salary")

#convert back to dynamic frame 
finalDF = DynamicFrame.fromDF(df, glueContext, 'finalDF')


finalDF = finalDF.repartition(1)


# Script generated for node Amazon S3
dq_result = EvaluateDataQuality().process_rows(frame=finalDF, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757275158368", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1757277458913 = glueContext.write_dynamic_frame.from_options(frame=dq_result, connection_type="s3", format="json", connection_options={"path": "s3://projectglueaw2025/Target/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1757277458913")

job.commit()