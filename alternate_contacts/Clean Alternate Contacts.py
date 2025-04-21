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

# Script generated for node Alternate Contacts
AlternateContacts_node1737570534525 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod",
    table_name="bloom_public_alternate_contact", 
    transformation_ctx="AlternateContacts_node1737570534525",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    })

# Script generated for node Change Schema
ChangeSchema_node1737570600608 = ApplyMapping.apply(frame=AlternateContacts_node1737570534525, mappings=[("agency", "string", "Alternate-agency", "string"), ("other_type", "string", "Alternate-other_type", "string"), ("id", "string", "AlternateContact-id", "string"), ("type", "string", "AlternateContact-type", "string")], transformation_ctx="ChangeSchema_node1737570600608")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1737570600608, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737570531138", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737571300500 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1737570600608, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-glue-assets-364076391763-us-west-1/alternate_contacts_no_pii/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1737571300500")

job.commit()