import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718128074888 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleansed", table_name="cleansed_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1718128074888")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718128082819 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleansed", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1718128082819")

# Script generated for node Join
Join_node1718128118778 = Join.apply(frame1=AWSGlueDataCatalog_node1718128074888, frame2=AWSGlueDataCatalog_node1718128082819, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1718128118778")

# Script generated for node Amazon S3
AmazonS3_node1718128206433 = glueContext.getSink(path="s3://analytics-youtube-data-eucentral1-dev/youtube/rpt_youtube_statistics_categories/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718128206433")
AmazonS3_node1718128206433.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="rpt_youtube_statistics_categories")
AmazonS3_node1718128206433.setFormat("glueparquet", compression="snappy")
AmazonS3_node1718128206433.writeFrame(Join_node1718128118778)
job.commit()