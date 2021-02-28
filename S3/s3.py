import getpass
import os
from typing import TypeVar, Optional
from .utils import make_directory_view, find_gitignore
import pandas as pd
from io import BytesIO
import json
import boto3
from time import time

Bucket = TypeVar('s3.Bucket')

class AWS_s3:
    def __init__(
                self, 
                region: str = 'ap-northeast-2', 
                bucket_name: str = 'rtm-ai',
                save_json: bool = True):
        
        '''
        aws = AWS_s3()
        
        variable
            aws.directory
            aws.bucket
            
        function
        
        '''
        secret_info_location = 'secret_info.json'
        valid_secret_info = True
        
        while True :
            if os.path.isfile(secret_info_location) and valid_secret_info:
                with open(secret_info_location, "r") as json_file: 
                    key_info = json.load(json_file)
                    aws_access_key_id = key_info['aws_access_key_id']
                    aws_secret_access_key = key_info['aws_secret_access_key']
                print('Loaded information successfully')
                
            else:
                aws_access_key_id = getpass.getpass('Enter your aws_access_key_id:')
                aws_secret_access_key = getpass.getpass('Enter your aws_secret_access_key:')

                key_info = {'aws_access_key_id' : aws_access_key_id,
                        'aws_secret_access_key' : aws_secret_access_key}
                
            self.bucket = self.__access_server(aws_access_key_id, aws_secret_access_key, region,
                                               bucket_name)
                
            if self.bucket is None:
                valid_secret_info = False
                continue
            
            if save_json:
                # save json
                with open(secret_info_location, "w") as json_file:
                    json.dump(key_info, json_file)
                    
                # add secret_info_location to .gitignore
                find_gitignore()
                
            break
        self.directory, self.file_list = make_directory_view(self.bucket)   
            
            
    def load(self, key: str) -> pd.DataFrame:
        file_name = key.split('/')[-1]
        extension = file_name.split('.')[-1]
        key, file_name, extension
        
        start_time = time()
        obj = self.bucket.Object(key=key)
        response = obj.get()
        lines = response['Body'].read()
        to_byte = BytesIO(lines)
        
        if extension in ['txt', 'csv']:
            try:
                df = pd.read_csv(to_byte, compression='gzip')
            except OSError:
                df = pd.read_csv(to_byte)
                print('gzip compression is recommended')

        elif extension in ['feather', 'ftr']:
            df = pd.read_feather(to_byte)

        elif extension == 'parquet':
            df = pd.read_parquet(to_byte)

        elif extension == 'xlsx':
            df = pd.read_excel(to_byte, engine='openpyxl')
            print('xlsx file is not recommended')
            
        elif extension in ['pickle', 'pkl']:
            df = pd.read_pickle(to_byte)
            
        else: 
            raise Exception("extension type '%s' is not valid" % extension)
        
        print('load time : %.2f sec' % (time() - start_time))
        return df

    def download(self, s3_path: str, local_path: str) -> None:
        self.bucket.download_file(s3_path, local_path)
        print('complete download!')
        
    def upload(self, local_path: str, s3_path: str):
        self.bucket.upload_file(local_path, s3_path)
        print('complete upload!')
    
    def __access_server(self, 
                        aws_access_key_id: str, 
                        aws_secret_access_key: str, 
                        region: str, 
                        bucket_name: str
                        ) -> Optional[Bucket]:
        
        session = boto3.Session(aws_access_key_id=aws_access_key_id, 
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region)
        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        
        try:
            list(bucket.objects.all())
            message = 'Connection success'
            
        except:
            message = 'aws key is wrong. please enter the key again.'
            bucket = None
            
        finally:
            print(message)
            return bucket
    
    
    