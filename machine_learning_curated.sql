CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`Machine_learning_curated` (
  `serialnumber` string,
  `timeStamp` bigint,
  `user` string,
  `x` float,
  `y` float,
  `z` float,
  `sensorReadingTime` bigint,
  `distanceFromObject` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://katya-spark-stedi-lake-house/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'json');
