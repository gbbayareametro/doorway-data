import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Birth Date->Age
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    selected_dynf = dfc.select(list(dfc.keys())[0])
    from pyspark.sql.functions import col, datediff, floor, current_date, to_date, concat, lit
    if not dfc.keys():
        print("No new data to process.")
        return DynamicFrameCollection({"results_dynf": DynamicFrame.fromDF(glueContext.spark_session.createDataFrame([], schema=StructType([])), glueContext, "empty")}, glueContext)

    selected_df = selected_dynf.toDF()
    required_columns = ['birth_month','birth_day','birth_year']
    if all(col in selected_df.columns for col in required_columns):
        selected_df = selected_df.withColumn('birthdate',
            to_date(concat(col('birth_year'), lit('-'), col('birth_month'), lit('-'), col('birth_day')))
        )
        selected_df = selected_df.withColumn('age',
            floor(datediff(current_date(), col('birthdate')) / 365)
        )
    else:
        print("No new applicant data...Skipping")

    results_dynf = DynamicFrame.fromDF(selected_df, glueContext, "Add Age")
    return DynamicFrameCollection({"results_dynf": results_dynf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1729545251521 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod",
    table_name="bloom_public_household_member",
    transformation_ctx="AWSGlueDataCatalog_node1729545251521",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    })

# Script generated for node Birth Date->Age
BirthDateAge_node1729545381383 = MyTransform(glueContext, DynamicFrameCollection({"AWSGlueDataCatalog_node1729545251521": AWSGlueDataCatalog_node1729545251521}, glueContext))

# Script generated for node Required by Glue
RequiredbyGlue_node1729545433015 = SelectFromCollection.apply(dfc=BirthDateAge_node1729545381383, key=list(BirthDateAge_node1729545381383.keys())[0], transformation_ctx="RequiredbyGlue_node1729545433015")

# Script generated for node Drop Fields
DropFields_node1729545526858 = DropFields.apply(frame=RequiredbyGlue_node1729545433015, paths=["created_at", "updated_at", "first_name", "middle_name", "last_name", "address_id", "work_address_id", "birth_year", "birth_month", "birth_day", "birthdate"], transformation_ctx="DropFields_node1729545526858")

# Script generated for node Amazon S3
AmazonS3_node1729547005998 = glueContext.getSink(path="s3://aws-glue-assets-364076391763-us-west-1/household_members_no_pii/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1729547005998")
AmazonS3_node1729547005998.setCatalogInfo(catalogDatabase="doorway-datalake",catalogTableName="household_members_no_pii")
AmazonS3_node1729547005998.setFormat("glueparquet", compression="snappy")
AmazonS3_node1729547005998.writeFrame(DropFields_node1729545526858)
job.commit()