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

# Script generated for node Customer Landing
CustomerLanding_node1746093467771 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://bucket-milenko/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1746093467771")

# Script generated for node Share With Research
SqlQuery0 = '''
select * from cl
where sharewithresearchasofdate is not null
'''
ShareWithResearch_node1745929032751 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cl":CustomerLanding_node1746093467771}, transformation_ctx = "ShareWithResearch_node1745929032751")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=ShareWithResearch_node1745929032751, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745929011776", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1745929036149 = glueContext.getSink(path="s3://bucket-milenko/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1745929036149")
CustomerTrusted_node1745929036149.setCatalogInfo(catalogDatabase="db-milenko",catalogTableName="customer_trusted")
CustomerTrusted_node1745929036149.setFormat("json")
CustomerTrusted_node1745929036149.writeFrame(ShareWithResearch_node1745929032751)
job.commit()