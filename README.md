# udacity_stedi_step
#### Navigate to AWS CloudShell
#### git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git
#### navigate to project/starter
#### make s3 bucket aws s3 ls s3://katya-spark-stedi-lake-house/
#### copy data to landing zone
#### accelerometer: aws s3 cp ./accelerometer s3://katya-spark-stedi-lake-house/accelerometer/landing/ --recursive
#### customer: aws s3 cp ./customer s3://katya-spark-stedi-lake-house/customer/landing/ --recursive
#### step_trainer: aws s3 cp ./step_trainer s3://katya-spark-stedi-lake-house/step_trainer/landing/ --recursive
#### create tables in Athena from S3 sources accelerometer/landing/ and customer/landing and save scripts
#### make screenshot of query result
#### create job source customer/landing transformation - filter on shareWithResearchAsOfDate column != 0 target customer/trusted
#### create job accelerometer_trusted
#### make customer_trusted table screenshot
#### create customer_curated job
#### create step_trainer_trusted job
#### create machine_learning_curated job
