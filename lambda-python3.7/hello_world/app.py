import datetime
import zipfile
import pandas as pd
import os
import boto3
import io
import json, subprocess


os.environ['KAGGLE_USERNAME'] = 'haoliangjiang1205'
os.environ['KAGGLE_KEY'] = "ac4e3ccf40f9b2359f39a47317af2d20"
os.environ['KAGGLE_CONFIG_DIR'] = '/tmp'

if not os.path.exists('/tmp'):
    os.makedirs('/tmp')
    
from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi()
api.authenticate()


def get_date_for_file():
    """returns date for downloading the most up-to-date data"""
    dtToday = datetime.datetime.today() 
    dtYesterday = dtToday - datetime.timedelta (days=1)
    if dtYesterday.month < 10:
        return '0'+str(dtYesterday.month)+str(dtYesterday.day)
    else:
        return str(dtYesterday.month)+str(dtYesterday.day)


def download_yesterday_data(api):
    print("start")
    api.dataset_download_file(dataset="bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows",
        file_name=get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip',
        path='/tmp')
    print('pass')

#     with zipfile.ZipFile(filename + zipname) as zf:
#         zf.extractall()
    with zipfile.ZipFile('/tmp/'+get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip.zip', 'r') as zipref:
        zipref.extractall('/tmp')
    print('pass2')
#     # read data into csv
    if os.path.exists('/tmp/'+get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip'):
        print('exists')
    #read part of the data or push the gzip to the s3
    train = pd.read_csv('/tmp/'+get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip', compression="gzip", index_col = 0)
    return train
def write_to_s3(data):
    #normally you don't need to pass in access key, you can give lambda the permission
    """write data to s3 bucket"""
    AWS_S3_BUCKET = os.getenv("721finalproj")
    AWS_ACCESS_KEY_ID = os.getenv("AKIAVWNNMDWO65JSPF7U")
    AWS_SECRET_ACCESS_KEY = os.getenv("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")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=False)
        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key="data.csv", Body=csv_buffer.getvalue()
            )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            return {
               "message": "Successful Pushed data to S3.",
                "status": status,
            }
        else:
            return {
                "message": "Was not successful in pushing data to S3.",
                "status": status,
            }
def lambda_handler(event, context):
    data = download_yesterday_data(api)
    write_to_s3(data)
