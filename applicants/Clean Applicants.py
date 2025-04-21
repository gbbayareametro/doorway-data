import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Birth Date -> Age
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

# Script generated for node Location Data
LocationData_node1729533195559 = glueContext.create_dynamic_frame.from_catalog(database="doorway-datalake", 
    table_name="address_with_census_tract", 
    transformation_ctx="LocationData_node1729533195559",
    additional_options={
        "jobBookmarkKeys": ["updated_at"],  # Replace "updated_at" with your timestamp/incremental column
        "jobBookmarksKeysSortOrder": "asc"
    })

# Script generated for node Applicant Table
ApplicantTable_node1729465000023 = glueContext.create_dynamic_frame.from_catalog(database="doorway-prod", table_name="bloom_public_applicant", transformation_ctx="ApplicantTable_node1729465000023")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1729533262025 = ApplyMapping.apply(frame=LocationData_node1729533195559, mappings=[("id", "string", "HomeAddress_id", "string"), ("created_at", "timestamp", "HomeAddress_created_at", "timestamp"), ("updated_at", "timestamp", "HomeAddress_updated_at", "timestamp"), ("place_name", "string", "HomeAddress_place_name", "string"), ("city", "string", "HomeAddress_city", "string"), ("county", "string", "HomeAddress_county", "string"), ("state", "string", "HomeAddress_state", "string"), ("street", "string", "HomeAddress_street", "string"), ("street2", "string", "HomeAddress_street2", "string"), ("zip_code", "string", "HomeAddress_zip_code", "string"), ("latitude", "decimal", "HomeAddress_latitude", "decimal"), ("longitude", "decimal", "HomeAddress_longitude", "decimal"), ("census_tract", "string", "HomeAddress_census_tract", "string")], transformation_ctx="RenamedkeysforJoin_node1729533262025")

# Script generated for node Join
ApplicantTable_node1729465000023DF = ApplicantTable_node1729465000023.toDF()
RenamedkeysforJoin_node1729533262025DF = RenamedkeysforJoin_node1729533262025.toDF()
Join_node1729533229416 = DynamicFrame.fromDF(ApplicantTable_node1729465000023DF.join(RenamedkeysforJoin_node1729533262025DF, (ApplicantTable_node1729465000023DF['address_id'] == RenamedkeysforJoin_node1729533262025DF['HomeAddress_id']), "left"), glueContext, "Join_node1729533229416")

# Script generated for node Birth Date -> Age
BirthDateAge_node1729534253582 = MyTransform(glueContext, DynamicFrameCollection({"Join_node1729533229416": Join_node1729533229416}, glueContext))

# Script generated for node Required By Glue
RequiredByGlue_node1729534655303 = SelectFromCollection.apply(dfc=BirthDateAge_node1729534253582, key=list(BirthDateAge_node1729534253582.keys())[0], transformation_ctx="RequiredByGlue_node1729534655303")

# Script generated for node Drop Fields
DropFields_node1729533339160 = DropFields.apply(frame=RequiredByGlue_node1729534655303, paths=["first_name", "birth_year", "phone_number", "created_at", "HomeAddress_id", "HomeAddress_latitude", "birth_day", "email_address", "updated_at", "HomeAddress_longitude", "middle_name", "address_id", "work_address_id", "last_name", "HomeAddress_created_at", "HomeAddress_place_name", "HomeAddress_street", "HomeAddress_street2", "birth_month", "birthdate"], transformation_ctx="DropFields_node1729533339160")

# Script generated for node Amazon S3
AmazonS3_node1729536714485 = glueContext.getSink(path="s3://aws-glue-assets-364076391763-us-west-1/applicants_no_pii/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1729536714485")
AmazonS3_node1729536714485.setCatalogInfo(catalogDatabase="doorway-datalake",catalogTableName="applicants_no_pii")
AmazonS3_node1729536714485.setFormat("glueparquet", compression="snappy")
AmazonS3_node1729536714485.writeFrame(DropFields_node1729533339160)
job.commit()