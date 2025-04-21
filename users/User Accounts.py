import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
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

# Script generated for node User Accounts
UserAccounts_node1741803559560 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod",
    table_name="bloom_public_user_accounts", 
    transformation_ctx="UserAccounts_node1741803559560",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    })

# Script generated for node Change Schema
ChangeSchema_node1741803604700 = ApplyMapping.apply(frame=UserAccounts_node1741803559560, mappings=[("last_login_at", "timestamp", "last_login_at", "timestamp"), ("created_at", "timestamp", "created_at", "timestamp"), ("password_updated_at", "timestamp", "password_updated_at", "timestamp"), ("mfa_enabled", "boolean", "mfa_enabled", "boolean"), ("id", "string", "id", "string"), ("first_name", "string", "first_name", "string"), ("email", "string", "email", "string"), ("last_name", "string", "last_name", "string")], transformation_ctx="ChangeSchema_node1741803604700")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1741803604700, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741803542853", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741803762534 = glueContext.getSink(path="s3://aws-glue-assets-364076391763-us-west-1/user_accounts/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1741803762534")
AmazonS3_node1741803762534.setCatalogInfo(catalogDatabase="doorway-datalake",catalogTableName="user_accounts")
AmazonS3_node1741803762534.setFormat("glueparquet", compression="snappy")
AmazonS3_node1741803762534.writeFrame(ChangeSchema_node1741803604700)
job.commit()