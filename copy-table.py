#!/usr/bin/python
import boto3

region = '<AWS_REGION'

# source parameters
source_db = "<source_database_name>"
source_table = "<source_table_name"
source_account = "<source_account_ID>"

# destination parameters. Set these values according to the use case. Depending on requirements, some of these values can be same as source.
dest_db = "<destination_database_name>" # Ensure that this database name already exists
dest_table = "<destination_table_name>" # If the table name needs to be same, set it to same as source_table
dest_account = "<destination_account_ID" # For same account, set it to same as source_account. For copying to different account, ensure that there is a Glue policy in place to grant required permissions

# Initiate Glue client. Instantiate another client if the source and destination regions are different. Then, use that client with creat_table and batch_create_partition methods.
client = boto3.client('glue',region_name=region)
getTable = client.get_table(
	CatalogId=source_account,
    DatabaseName=source_db,
    Name=source_table)

# check for successful response
if getTable:
	table = getTable['Table']

	# update table name if table is renamed
	table['Name'] = dest_table

	# remove unnecessary attributes, which can cause validation exception with creation/update of table
	if 'UpdateTime' in table: del table['UpdateTime']
	if 'CreatedBy' in table: del table['CreatedBy']
	if 'CreateTime' in table: del table['CreateTime']
	if 'DatabaseName' in table: del table['DatabaseName']
	if 'IsRegisteredWithLakeFormation' in table: del table['IsRegisteredWithLakeFormation']

	createTable = client.create_table(
		CatalogId=dest_account,
		DatabaseName=dest_db,
	    TableInput=table)

	if createTable:
		print("Table " + dest_table + " copied to database: " + dest_db)

if table['PartitionKeys']:
	print("Table has partitions, so copying them as well.")

	# For large number of partitions, GetPartitions API will return partial no. of partitions at a time, so need to recurse through using NextToken
	request_count = 0
	while True:
		request_count = request_count + 1
		# for first request
		if request_count == 1 :
			getPartitions = client.get_partitions(
				CatalogId=source_account,
	        	DatabaseName=source_db,
	        	TableName=source_table
    		)
	    # For subsequent requests. Specify next token to get next page of partitions.
		else:
			getPartitions = client.get_partitions(
				CatalogId=source_account,
		        DatabaseName=source_db,
		        TableName=source_table,
		        NextToken=nextToken
			)

		# check for successful response
		if getPartitions:
			partitions = getPartitions['Partitions']

			# Drop unnecessary attributes from each partition
			for partition in partitions:
				del partition['TableName']
				del partition['DatabaseName']
				del partition['CreationTime']

			# Create partitions in new table
			createPartitions = client.batch_create_partition(
				CatalogId=dest_account,
				DatabaseName=dest_db,
			    TableName=dest_table,
			    PartitionInputList=partitions)

		# check presence of next token to exit the loop
		if "NextToken" in getPartitions:
			nextToken = getPartitions['NextToken']
		else:
			print("Partitions copied. Exiting")
			break