import datetime
import zipfile
import pandas as pd
import os

os.environ['KAGGLE_USERNAME'] = "haoliangjiang1205"
os.environ['KAGGLE_KEY'] = "b85cdfe8b2c0cf3e91ad16c5f1e01bf1"
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
    train = pd.read_csv('/tmp/'+get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip', compression="gzip", nrows=10, index_col = 0)
    print(train.head())
    print('Done')

def lambda_handler(event, context):
    download_yesterday_data(api)
    #push_to_s3()