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
Step_trainer_landing_node1695891570140 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1695891570140",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1",
)

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1695891349379 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://katya-spark-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1695891349379",
)

# Script generated for node Cust_Accel_Join
Cust_Accel_Join_node1695891420608 = Join.apply(
    frame1=Customer_Trusted_node1,
    frame2=Accelerometer_Landing_node1695891349379,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Cust_Accel_Join_node1695891420608",
)

# Script generated for node Join
Join_node1695891643972 = Join.apply(
    frame1=Cust_Accel_Join_node1695891420608,
    frame2=Step_trainer_landing_node1695891570140,
    keys1=["serialNumber", "timeStamp"],
    keys2=["serialNumber", "sensorReadingTime"],
    transformation_ctx="Join_node1695891643972",
)

# Script generated for node Drop Fields
DropFields_node1695892187616 = DropFields.apply(
    frame=Join_node1695891643972,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1695892187616",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1695892265463 = DynamicFrame.fromDF(
    DropFields_node1695892187616.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1695892265463",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1695892352303 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropDuplicates_node1695892265463,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://katya-spark-stedi-lake-house/machine_learning_curated/",
            "partitionKeys": [],
        },
        transformation_ctx="machine_learning_curated_node1695892352303",
    )
)

job.commit()
