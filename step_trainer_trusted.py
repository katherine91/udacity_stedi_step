import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1695883326489 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1695883326489",
)

# Script generated for node CustomerCurated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1695883667973 = DynamicFrame.fromDF(
    CustomerCurated_node1.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicates_node1695883667973",
)

# Script generated for node Join
Join_node1695883780786 = Join.apply(
    frame1=DropDuplicates_node1695883667973,
    frame2=Step_trainer_landing_node1695883326489,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1695883780786",
)

# Script generated for node Drop Fields
DropFields_node1695883840020 = DropFields.apply(
    frame=Join_node1695883780786,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1695883840020",
)

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1695883932352 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695883840020,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://katya-spark-stedi-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Step_trainer_trusted_node1695883932352",
)

job.commit()
