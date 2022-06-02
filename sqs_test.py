import logging
import boto3
from botocore.exceptions import ClientError
import json
import os
import glob
import shutil
import time

AWS_REGION = 'eu-west-3'

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

sqs_client = boto3.client("sqs", region_name=AWS_REGION,aws_access_key_id="AKIA2QIPY2A4A32XVMPT",aws_secret_access_key="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")


folder = "Recordings"
file = "output.mp4"

def make_folder():
    try:
        print("making a folder")
        os.mkdir(folder)
    except FileExistsError:
        print("Folder already exist")

def receive_queue_message(queue_url):
    """
    Retrieves one or more messages (up to 10), from the specified queue.
    """
    try:
        response = sqs_client.receive_message(QueueUrl=queue_url)
    except ClientError:
        logger.exception(
            f'Could not receive the message from the - {queue_url}.')
        raise
    else:
        return response


def delete_queue_message(queue_url, receipt_handle):
    """
    Deletes the specified message from the specified queue.
    """
    try:
        response = sqs_client.delete_message(QueueUrl=queue_url,
                                             ReceiptHandle=receipt_handle)
    except ClientError:
        logger.exception(
            f'Could not delete the message from the - {queue_url}.')
        raise
    else:
        return response

def get_video():
    print("getting video")
    download = "s3cmd get s3://" + name_bucket + "/" + name_video + " /home/admin/"+ folder
    os.system(download)

def downscaling():
    os.chdir("/home/admin/" + folder + "/")
    os.system("echo Checking if recordings are found:")
    recording_list = glob.glob("*.mp4")

    print("Starting downsizing")
    os.system(f"ffmpeg -i {recording_list[0]} -vf scale=1920:1080 output.mp4")
    print("finished")

def uploading():
    print("uploading")
    upload = "s3cmd put /home/admin/" + folder + "/" + file + " s3://democonvertion2/" + name_video
    os.system(upload)

def delete():
    print("deleting")
    files = glob.glob('/home/admin/Recordings/*')
    for f in files:
        os.remove(f)


def main():
    while True:
        global name_video
        global name_bucket
        QUEUE_URL = 'https://sqs.eu-west-3.amazonaws.com/722124460088/bob'
        messages = receive_queue_message(QUEUE_URL)
        try:
            key = messages["Messages"][0]["Body"]
            data = json.loads(key)
            name_video = data["Records"][0]["s3"]["object"]["key"]
            name_bucket = data["Records"][0]["s3"]["bucket"]["name"]

            for msg in messages['Messages']:
                msg_body = msg['Body']
                receipt_handle = msg['ReceiptHandle']

                logger.info(f'The message body: {msg_body}')

                logger.info('Deleting message from the queue...')

                delete_queue_message(QUEUE_URL, receipt_handle)

            logger.info(f'Received and deleted message(s) from {QUEUE_URL}.')

        except KeyError:
            time.sleep(5)
            main()
        try:
            get_video()
            downscaling()
            uploading()
            delete()

        except Exception as e:
            print(e)
            main()

if __name__ == '__main__':
    make_folder()
    main()
