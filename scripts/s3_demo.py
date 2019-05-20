import boto3
import sys
import os


BUCKET_NAME = os.getenv("BUCKET_NAME", None)
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID", None)
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY", None)


class S3Wrapper(object):
    def __init__(self):
        # get a s3 client
        s3_client = boto3.resource('s3', 
                aws_access_key_id=ACCESS_KEY_ID, 
                aws_secret_access_key=SECRET_ACCESS_KEY)

        # connect with the bucket
        self.training_bucket = s3_client.Bucket(BUCKET_NAME)

    def download_training_data(self, output_dir):
        # iterate through the objects and download the files
        for obj in self.training_bucket.objects.all():

            s3_path, filename = os.path.split(obj.key)
            local_dir_path = os.path.join(output_dir, s3_path)
            local_file_path = os.path.join(local_dir_path, filename)

            # create the local directory tree
            if not os.path.exists(local_dir_path):
                os.makedirs(local_dir_path)

            # download file if not downloaded earlier
            if not os.path.exists(local_file_path):
                print('Downloading %s' % obj.key)
                self.training_bucket.download_file(obj.key, local_file_path)
                # stop iteration
                # sys.exit()


if __name__ == '__main__':
    s3_wrapper = S3Wrapper()
    s3_wrapper.download_training_data('training-data')
