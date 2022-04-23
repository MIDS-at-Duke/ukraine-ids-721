import boto3
import pandas
import os

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name = 'us-east-1'
)
    
# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name = 'us-east-1'
)

# Fetch the list of existing buckets
clientResponse = client.list_buckets()
    
# Print the bucket names one by one
print('Printing bucket names...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

# Create the S3 object
obj = client.get_object(
    Bucket = 'ukrainedata',
    Key = 'data.csv'
)

# Read data from the S3 object
data = pandas.read_csv(obj['Body'], lineterminator='\n')
    
# Print the data frame
print('Printing the data frame...')
print(data)

# print ("Long time no see!")
