### installation

    conda create -n env_name python=3.7
    conda activate env_name
    pip install -r requirements.txt

### Example
    from S3 import AWS_s3
    aws = AWS_s3()

    print(aws.directory)

    # load data and convert file to dataframe
    aws.load(path)
    
    # download s3 file to local folder
    aws.download(s3_path, local_path)

    # upload local file to s3 server
    aws.upload(local_path, s3_path)