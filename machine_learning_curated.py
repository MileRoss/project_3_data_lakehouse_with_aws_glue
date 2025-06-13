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
AccelerometerTrusted_node1745956762034 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745956762034")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1745956762480 = glueContext.create_dynamic_frame.from_catalog(database="db-milenko", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1745956762480")

# Script generated for node Join
SqlQuery2275 = '''
select * from stt
join at on stt.sensorreadingtime = at.timestamp
'''
Join_node1745956764716 = sparkSqlQuery(glueContext, query = SqlQuery2275, mapping = {"stt":StepTrainerTrusted_node1745956762480, "at":AccelerometerTrusted_node1745956762034}, transformation_ctx = "Join_node1745956764716")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=Join_node1745956764716, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745956757164", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1745956774481 = glueContext.getSink(path="s3://bucket-milenko/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1745956774481")
MachineLearningCurated_node1745956774481.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1745956774481.setFormat("json")
MachineLearningCurated_node1745956774481.writeFrame(Join_node1745956764716)
job.commit()