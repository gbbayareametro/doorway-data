import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Add Census Tract
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    def getCensusTract(street, city , state):
        import boto3
        import json
        import re
        client = boto3.client('lambda')
        payload = json.dumps({"street":street,"city": city,"state":state})
        response = client.invoke(FunctionName='census-tract-finder-prod',
            InvocationType='RequestResponse',
            Payload = payload)
        census_tract = response["Payload"].read().decode('utf-8')
       
        return census_tract.strip('"')
    from pyspark.sql.functions import col, lit, udf
    from pyspark.sql.types import StringType
    getCensusTract_udf = udf(getCensusTract, StringType())
    selected_dynf = dfc.select(list(dfc.keys())[0])
    selected_df = selected_dynf.toDF()
    selected_df = selected_df.withColumn('census_tract', getCensusTract_udf(col('street'), col('city'),col('state')))
    print("\nTransformed Data Frame:")
    selected_df.select("census_tract").show(5)
    results_dynf = DynamicFrame.fromDF(selected_df, glueContext, "Add Census Tract")
    return DynamicFrameCollection({"results_dynf": results_dynf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Address Table
AddressTable_node1729196797456 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod", 
    table_name="bloom_public_address", 
    transformation_ctx="AddressTable_node1729196797456",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    })

# Script generated for node Add Census Tract
AddCensusTract_node1729196855438 = MyTransform(glueContext, DynamicFrameCollection({"AddressTable_node1729196797456": AddressTable_node1729196797456}, glueContext))

# Script generated for node Needed for Glue
NeededforGlue_node1729204441708 = SelectFromCollection.apply(dfc=AddCensusTract_node1729196855438, key=list(AddCensusTract_node1729196855438.keys())[0], transformation_ctx="NeededforGlue_node1729204441708")

# Script generated for node Amazon S3
AmazonS3_node1729204476655 = glueContext.getSink(path="s3://aws-glue-assets-364076391763-us-west-1/address_with_census_tract/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1729204476655")
AmazonS3_node1729204476655.setCatalogInfo(catalogDatabase="doorway-datalake",catalogTableName="address_with_census_tract")
AmazonS3_node1729204476655.setFormat("glueparquet", compression="snappy")
AmazonS3_node1729204476655.writeFrame(NeededforGlue_node1729204441708)
job.commit()