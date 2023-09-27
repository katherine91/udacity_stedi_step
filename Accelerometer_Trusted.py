import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Landing
Customer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Landing_node1",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1695813381017 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1695813381017",
)

# Script generated for node Join
Join_node1695813473676 = Join.apply(
    frame1=Customer_Landing_node1,
    frame2=Accelerometer_Landing_node1695813381017,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695813473676",
)

# Script generated for node Filter
Filter_node1695813588739 = Filter.apply(
    frame=Join_node1695813473676,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1695813588739",
)

# Script generated for node Drop Fields
DropFields_node1695813577468 = DropFields.apply(
    frame=Filter_node1695813588739,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1695813577468",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695813577468,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://katya-spark-stedi-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometer_Trusted_node2",
)

job.commit()
