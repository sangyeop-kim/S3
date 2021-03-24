import numpy as np
from typing import Tuple, List, TypeVar
import json
import boto3
import os

Bucket = TypeVar('s3.Bucket')

def feature(name: str) -> Tuple[str, str, bool, int]:
    prefix = ('/').join(name.split('/')[:-1]) + '/'
    folder = (name.split('/')[-1]) == ''
    length = len([i for i in prefix.split('/') if len(i) > 0])
    
    return name, prefix, folder, length



def make_directory_view(bucket: Bucket) -> Tuple[str, List[str]]:
    file_list = list(map(lambda x : x.key, bucket.objects.all()))

    unique_dir = []
    new_file_list =[]
    all_folder = []
    all_length = []

    for num, f in enumerate(file_list):
        root_folder = ('/').join(f.split('/')[:-1]) + '/'
        unique_dir.append(f)
        
        if  root_folder not in unique_dir :
            feature_ = feature(f)    
            new_file_list.append((feature_[1], feature_[1], True, feature_[3]))
            all_folder.append(True)
            all_length.append(feature_[-1])
            unique_dir.append(root_folder)
        
        feature_ = feature(f)
        all_folder.append(feature_[-2])
        all_length.append(feature_[-1])
        new_file_list.append(feature(f))

    
    new_file_list.append(('', '', True, 1))
    all_folder.append(True)
    all_length.append(1)
    all_folder = np.array(all_folder)
    all_length = np.array(all_length)

    start_idx = np.hstack((np.where(all_folder == True)[0], -1))
    end_idx = np.hstack((np.diff(start_idx[:-1]), len(all_folder)-start_idx[-2], -1)) + start_idx

    output_str = ''
    for num1, (start, end) in enumerate(zip(start_idx[:-1], end_idx[:-1])):
        
        next_folder_len = new_file_list[start_idx[num1+1]][-1] # 다음 폴더 길이는?
        
        if all_length[start] == 1:
            last_folder = False
            
        for num2, file in enumerate(new_file_list[start:end]):
            name, prefix, folder, length = file
            last_file = False
            
            if num2 + 1 == len(new_file_list[start:end]):
                last_file = True
            
            if not last_folder:
                condition1 = (all_length[start:start+np.where(all_length[start:] == 1)[0][0]] == 2)
                condition2 = (all_folder[start:start+np.where(all_length[start:] == 1)[0][0]])
                
                if np.sum(condition1 & condition2) == 1:
                    last_folder = True
            
            if num1 != 0 and file[-1] == 1: #대분류 시작 엔터
                output_str += '\n'
            
            if length == 1: # 길이가 1일 때
                if folder:
                    output_str += name + '\n'
                else:
                    if last_file:
                        output_str += '└── '+ name + '\n'
                    else:
                        output_str += '├── ' + name + '\n'
            
                '''To do'''
            elif last_folder: # 대분류 마지막인지?
                if folder: # 폴더인지
                    output_str += '     ' * (length-2) + '│    ' + '\n'
                    output_str += '     ' * (length-2) + '└── ' + name + '\n'
                    
                else:
                    if last_file:
                        output_str += '     ' * (length-1) + '└── ' + name + '\n'
                    else:
                        output_str += '     ' * (length-1) + '├── ' + name + '\n'
                    
            else: 
                if folder: # 폴더인지
                    if next_folder_len < length: # 밑에 폴더 더 있는지 확인
                        output_str += '│   ' * (length-2) + '│    ' + '\n'
                        output_str += '│   ' * (length-2) + '└── ' + name + '\n'

                    else:
                        output_str += '│   ' * (length-2) + '│    ' + '\n'
                        output_str += '│   ' * (length-2) + '├── ' + name + '\n'

                else:
                    if last_file: 
                        if next_folder_len <= length:# 밑에 폴더 더 있는지 확인
                            if length == 2:
                                output_str += '│   ' + '└── ' + name + '\n'
                            else: 
                                output_str += '│   ' * (length-2) + '     ' + '└── ' + name +'\n'
                        else:
                            output_str += '│   ' * (length-1) + '├── ' + name + '\n'
                    else:
                        output_str += '│   ' * (length-1) + '├── ' + name + '\n'
                        
    return output_str, [i[0] for i in new_file_list[:-1]]


def write_info_in_gitignore(path: str) -> bool:
    if os.path.isdir(path):
        git_path = path[:-4]

        if os.path.isfile(git_path + '.gitignore'):
            # open gitignore
            with open(git_path + '.gitignore', 'r') as f:
                ignore_list = f.read().splitlines()

            # write secret_info.json in .gitignore
            if not 'secret_info.json' in ignore_list:
                with open(git_path + '.gitignore', 'a') as f:
                    f.write('\n')
                    f.write('secret_info.json')

        else:
            # make .gitignore file and write secret_info.json
            with open(git_path + '.gitignore', 'w') as f:
                f.write('secret_info.json')
        return True
    
    else:
        return False
    
    
def find_gitignore() -> None:
    path_list = ['.git', '../.git', '../../.git']
    find_ok = False
    
    for path in path_list:
        write = write_info_in_gitignore(path)
        
        if write:
            find_ok = True
            break
            
    if find_ok == False:
        print("if you use git in the future, you must add secret_info.json in .gitignore!!")


                        
if __name__ == '__main__':
    
    with open("../secret_info.json", "r") as json_file:
        key_info = json.load(json_file)
        aws_access_key_id = key_info['aws_access_key_id']
        aws_secret_access_key = key_info['aws_secret_access_key']
    
    print('accessed successfully.')
    
    region = 'ap-northeast-2'
    bucket_name = 'rtm-ai'
    session = boto3.Session(
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=region
                            )
    print(make_directory_view(session.resource('s3').Bucket(bucket_name)))
    