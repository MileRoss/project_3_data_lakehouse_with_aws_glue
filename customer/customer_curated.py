import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745951462787 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745951462787")

# Script generated for node Customer Trusted
CustomerTrusted_node1745951463089 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745951463089")

# Script generated for node Join
SqlQuery2166 = '''
select distinct ct.* from ct
join at on ct.email = at.user
'''
Join_node1745951465354 = sparkSqlQuery(glueContext, query = SqlQuery2166, mapping = {"ct":CustomerTrusted_node1745951463089, "at":AccelerometerTrusted_node1745951462787}, transformation_ctx = "Join_node1745951465354")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=Join_node1745951465354, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745951456954", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1745951474121 = glueContext.getSink(path="s3://bucket-milenko/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1745951474121")
CustomerCurated_node1745951474121.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="customer_curated")
CustomerCurated_node1745951474121.setFormat("json")
CustomerCurated_node1745951474121.writeFrame(Join_node1745951465354)
job.commit()