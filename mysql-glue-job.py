import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
# from pyspark.sql import SQLContext

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
# sqlContext = SQLContext(sc)

spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# variables
rds_endpoint = "<endpoint>.rds.amazonaws.com"
db_name = ""
db_table = ""
db_user = ""
db_password = "<password>"
jar_location = "s3://<bucket>/mysql-connector-java-8.0.17.jar" 

## @type: DataSource
## @args: [database = "<db_name>", table_name = "<table_name>", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
## datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "<db_name>", table_name = "<table_name>", transformation_ctx = "datasource0")
connection_mysql8_options = {
    "url": "jdbc:mysql://" +rds_endpoint+ ":3306/" +db_name,
    "dbtable": db_table,
    "user": db_user,
    "password": db_password,
    "customJdbcDriverS3Path": jar_location,
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver",
    "hashfield": "id",
    "hashpartitions": "10"
}
# datasource0 = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql8_options)
# datasource0.getNumPartitions()

jdbc_url = "jdbc:mysql://" + rds_endpoint + ":3306/" +db_name+ "?user=" +db_user "&password=" + db_password
query = "(select * from test where gender='male') t1_alias"
mydf = glueContext.read.format('jdbc').options(driver='com.mysql.cj.jdbc.Driver',url=jdbc_url, dbtable=query ).load()
datasource0 = DynamicFrame.fromDF(mydf, glueContext, "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("type", "string", "type", "string"), ("id", "string", "id", "string"), ("name", "string", "name", "string"), ("identifiers", "array", "identifiers", "array"), ("other_names", "array", "other_names", "array")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("family_name", "string", "family_name", "string"), ("name", "string", "name", "string"), ("gender", "string", "gender", "string"), ("image", "string", "image", "string"), ("given_name", "string", "given_name", "string"), ("birth_date", "string", "birth_date", "string"), ("id", "string", "id", "string"), ("death_date", "string", "death_date", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://<bucket>/output2/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://<bucket>/output/"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()