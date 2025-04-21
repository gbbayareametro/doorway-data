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

# Script generated for node Applications Table
ApplicationsTable_node1745253703504 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "applications",
        "connectionName": "doorway-prod-con",
    },
    transformation_ctx = "ApplicationsTable_node1745253703504",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    }
)

# Script generated for node Applications
Applications_node1737590375588 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod", table_name="bloom_public_applications", transformation_ctx="Applications_node1737590375588")

# Script generated for node Change Schema
ChangeSchema_node1738352079330 = ApplyMapping.apply(frame=Applications_node1737590375588, mappings=[("id", "string", "Application-id", "string"), ("app_url", "string", "Application-app_url", "string"), ("contact_preferences", "array", "contact_preferences", "array"), ("household_size", "int", "household_size", "int"), ("send_mail_to_mailing_address", "boolean", "send_mail_to_mailing_address", "boolean"), ("household_expecting_changes", "boolean", "household_expecting_changes", "boolean"), ("household_student", "boolean", "household_student", "boolean"), ("income", "string", "income", "string"), ("preferences", "string", "preferences", "string"), ("programs", "string", "programs", "string"), ("submission_date", "timestamp", "submission_date", "timestamp"), ("user_id", "string", "user_id", "string"), ("listing_id", "string", "listing_id", "string"), ("applicant_id", "string", "applicant_id", "string"), ("alternate_contact_id", "string", "alternate_contact_id", "string"), ("accessibility_id", "string", "accessibility_id", "string"), ("demographics_id", "string", "demographics_id", "string"), ("income_vouchers", "array", "income_vouchers", "array"), ("income_period", "string", "income_period", "string"), ("submission_type", "string", "submission_type", "string")], transformation_ctx="ChangeSchema_node1738352079330")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1738352079330, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738351117958", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1738352476262 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1738352079330, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-glue-assets-364076391763-us-west-1/applications_no_pii/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1738352476262")

job.commit()