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

# Script generated for node Customer Curated
CustomerCurated_node1745950128256 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="customer_curated", transformation_ctx="CustomerCurated_node1745950128256")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1746093775889 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://bucket-milenko/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1746093775889")

# Script generated for node Join
SqlQuery0 = '''
select * from stl
join cc on stl.serialnumber = cc.serialnumber
'''
Join_node1745950130171 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cc":CustomerCurated_node1745950128256, "stl":StepTrainerLanding_node1746093775889}, transformation_ctx = "Join_node1745950130171")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1745950130171, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745950094784", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1745950132803 = glueContext.getSink(path="s3://bucket-milenko/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1745950132803")
StepTrainerTrusted_node1745950132803.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1745950132803.setFormat("json")
StepTrainerTrusted_node1745950132803.writeFrame(Join_node1745950130171)
job.commit()