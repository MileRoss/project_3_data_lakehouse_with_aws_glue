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

# Script generated for node Customer Trusted
CustomerTrusted_node1745930402655 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745930402655")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1746093633106 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://bucket-milenko/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1746093633106")

# Script generated for node Join
SqlQuery0 = '''
select al.* from al
join ct on al.user = ct.email
'''
Join_node1745930406021 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ct":CustomerTrusted_node1745930402655, "al":AccelerometerLanding_node1746093633106}, transformation_ctx = "Join_node1745930406021")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1745930406021, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745929011776", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1745930424237 = glueContext.getSink(path="s3://bucket-milenko/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1745930424237")
AccelerometerTrusted_node1745930424237.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1745930424237.setFormat("json")
AccelerometerTrusted_node1745930424237.writeFrame(Join_node1745930406021)
job.commit()