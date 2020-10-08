import logging
import os
import io
import boto3
import botocore
from botocore.exceptions import ClientError
from werkzeug.utils import secure_filename

class S3Handler():
    
    def __init__(self, AWS_REGION=None, AWS_ACCESS_KEY=None, AWS_SECRET_KEY=None):
        self.awsRegion = AWS_REGION
        self.awsAccessKey = AWS_ACCESS_KEY
        self.awsSecretKey = AWS_SECRET_KEY
        self.s3 = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        self.s3Resource = boto3.resource('s3',
        region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY)
    
    def createBucket(self, bucket_name):  
        try:
            location = {'LocationConstraint': self.awsRegion}
            self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def listBuckets(self):
        response = self.s3.list_buckets()
        # Output the bucket names
        print('Existing buckets:', flush=True)
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}', flush=True)
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets

    def uploadFile(self, bucket_name, filename):
        try:
            self.s3.upload_file(filename, bucket_name, filename, ExtraArgs={'ACL':'public-read'})
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def downloadFile(self, bucketName, key):
        '''
        try:
            return self.s3Resource.Bucket(bucketName).download_file(key, 'name')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        '''
        try:
            #self.s3.download_fileobj(bucketName, key, inMemoryFile)
            obj = self.s3.get_object(Bucket=bucketName, Key=key)
            return io.BytesIO(obj['Body'].read())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                return False

    def uploadFileObject(self, data, bucket_name, filename, useResource = True):
        #useResource = True
        response = None
        if useResource:
            try:
                response = self.s3Resource.Object(bucket_name, filename).put(Body=data, ACL='public-read')
            except ClientError as e:
                logging.error(e)
                return False
        else:
            try:
                response = self.s3.put_object(Body=data, Bucket=bucket_name, Key=filename, ExtraArgs={'ACL':'public-read'})
            except ClientError as e:
                logging.error(e)
                return False  
        return response
    
    def listObjectsInBucket(self, bucketName, prefix):
        result = self.s3.list_objects(Bucket = bucketName, Prefix=prefix, Delimiter='/')
        keys = []
        if 'Contents' in result:
            for o in result['Contents']:
                keys.append(o['Key'])
        return keys

    def storeDataLocally(self, _file, filename):
        useSimple = True
        filenameWithPath = os.path.join('/home' , secure_filename(filename))
        if useSimple:
            _file.stream.seek(0)
            _file.save(filenameWithPath)
        else:
            inMemoryFile = io.BytesIO(_file.read())
            inMemoryFile.seek(0)
            self.writeToFile(filenameWithPath, inMemoryFile)
    
    def writeToFile(self, filename, bytesio):
        """
        Write the contents of the given BytesIO to a file.
        Creates the file or overwrites the file if it does
        not exist yet. 
        """
        with open(filename, "wb") as outfile:
            # Copy the BytesIO stream to the output file
            outfile.write(bytesio.getbuffer())