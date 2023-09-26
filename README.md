# udacity_stedi_step
Navigate to AWS CloudShell
git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git
navigate to project/starter
make s3 bucket aws s3 ls s3://katya-spark-stedi-lakehouse/
copy data to landing zone
accelerometer: aws s3 cp ./accelerometer s3://katya-spark-stedi-lakehouse/accelerometer/landing/ --recursive
customer: aws s3 cp ./customer s3://katya-spark-stedi-lakehouse/customer/landing/ --recursive
step_trainer: aws s3 cp ./step_trainer s3://katya-spark-stedi-lakehouse/step_trainer/landing/ --recursive
