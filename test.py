import datetime
import zipfile
import pandas as pd

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
    api.dataset_download_file(dataset="bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows",
        file_name=get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip',
        path='./')

#     with zipfile.ZipFile(filename + zipname) as zf:
#         zf.extractall()
    with zipfile.ZipFile(get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip.zip', 'r') as zipref:
        zipref.extractall()
#     # read data into csv
    train = pd.read_csv(get_date_for_file()+'_UkraineCombinedTweetsDeduped.csv.gzip', compression="gzip")
    print(train.head())

if __name__ == '__main__':
    download_yesterday_data(api)