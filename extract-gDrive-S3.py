##################################################################################################################
# Author        | Zuhaib Muhammad                                                                                #
# Email         | zuhaib.muhammad49@gmail.com                                                                    #
# Github        | https://github.com/kunzuhaIB/googleDrive-to-S3                                                 #
# Description   | Pipeline to transfer the files from Google Drive to AWS S3 bucket.                             #
##################################################################################################################

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from logging import exception
import pandas as pd
import datetime as dt
from io import StringIO
import os
 
S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
access_token = os.getenv('access_token')
bucket = os.getenv('bucket')
g_drive_folder = os.getenv('g_drive_folder')

# Authenticating google drive to access the required files.
def google_drive():
    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)
    return drive

# Getting the required files in an object from the google drive using the mimetype as text/csv with the query.
def get_files(drive, folder_id):
    list_files = drive.ListFile({'q' : f"'{folder_id}' in parents and trashed=false and mimeType='text/csv'"}).GetList()
    return list_files

# Uploading the files to S3.
def upload_to_s3(files_obj):
     for file in files_obj:
        str_get = file.GetContentString(file['title'])
        df = pd.read_csv(StringIO(str_get), sep=',')
        tg_path = 's3://' + bucket + '/' + file['title']
        df.to_csv(tg_path, storage_options={'key': S3_ACCESS_KEY_ID, 'secret': S3_SECRET_ACCESS_KEY})


def main():
    try:
        print(str(dt.datetime.now()) + ": Checking for files in drive.")
        try:
            drive = google_drive()
            print(str(dt.datetime.now()) + ": Files are found in the drive.")
        except exception as e:
            print(str(dt.datetime.now()) + ': Error while checking the files in the google drive.')
        
        print(str(dt.datetime.now()) + ": Getting the files from the drive.") 
        try:
            files = get_files(drive, g_drive_folder)
        except exception as e:
            print(str(dt.datetime.now()) + ': Error while getting the files from the drive.')
        upload_to_s3(files)
        print(str(dt.datetime.now()) + ': All files have been transfered successfully.')
    except exception as e:
        print(str(dt.datetime.now()) + ': Error while uploading files: ' + e)

main()