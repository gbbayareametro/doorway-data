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

# Script generated for node Listings
Listings_node1738195980773 = glueContext.create_dynamic_frame.from_catalog(
    database="doorway-prod", 
    table_name="bloom_public_listings", 
    transformation_ctx="Listings_node1738195980773",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    }
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Listings_node1738195980773, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738195969640", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1738196073397 = glueContext.write_dynamic_frame.from_options(frame=Listings_node1738195980773, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-glue-assets-364076391763-us-west-1/listings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1738196073397")

job.commit()