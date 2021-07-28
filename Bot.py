import requests
import json
import boto3
import os
import sys
from requests.sessions import session
from requests_oauthlib import OAuth1
from time import sleep

def main(event, lambda_context):
    #various keys for twitter api
    twitter_consumer_key = os.environ['twitter_consumer_key']
    twitter_consumer_secret = os.environ['twitter_consumer_secret']
    twitter_access_token = os.environ['twitter_access_token']
    twitter_access_token_secret = os.environ['twitter_access_token_secret']
    
    auth = OAuth1(twitter_consumer_key, twitter_consumer_secret,
                twitter_access_token, twitter_access_token_secret)
    
    MEDIA_ENDPOINT_URL = 'https://upload.twitter.com/1.1/media/upload.json'
    POST_TWEET_URL = 'https://api.twitter.com/1.1/statuses/update.json'

    S3_BUCKET = 'tntkabukichick'
    S3_KEY = 'Tntkabukichick.mp4'
    
    #get the s3 content(will be either png or mp4)
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    if 'png' in S3_KEY.lower():
        media_type = 'image/png'
    else:
        media_type = 'video/mp4'

    #create requests session for twitter calls
    ts = requests.session()
    ts.auth = auth
    
    #INIT command upload for twitter media
    upload_init_req_data = {
        'command' : 'INIT',
        'media_type' : media_type,
        'total_bytes' : s3_object['ContentLength']
    }
    resp = ts.post(url=MEDIA_ENDPOINT_URL, data=upload_init_req_data)
    if not resp.ok:
        print('Failed to init upload to twitter')
        print(resp.status_code)
        print(resp.text)
        sys.exit(1)
    media_id = resp.json()['media_id']
    
    #begin sending file in chunks with append command
    segment_id = 0
    bytes_sent = 0
    while bytes_sent < s3_object['ContentLength']:
        chunk = s3_object['Body'].read(4*1024*1024)
        #append command for twitter media
        data = {
            'command' : 'APPEND',
            'media_id' : media_id,
            'segment_index' : segment_id
        }
        files = {
            'media' : chunk
        }
        resp = ts.post(url=MEDIA_ENDPOINT_URL, data=data, files=files)
        if resp.status_code < 200 or resp.status_code > 299:
            print('Failed to append media')
            print(resp.status_code)
            print(resp.text)
            sys.exit(1)
        segment_id += 1
        bytes_sent = 4*1024*1024*segment_id
    
    #finalize command for twitter media upload
    finalize_data = {
        'command' : 'FINALIZE',
        'media_id' : media_id
    }
    resp = ts.post(url=MEDIA_ENDPOINT_URL, data=finalize_data)
    if not resp.ok:
        print('Failed to finalize media')
        print(resp.status_code)
        print(resp.text)
    resp_json = resp.json()
    
    result = 'succeeded'
    #check if we need to wait for media to complete uploading
    if 'processing_info' in resp_json:
        #wait until media has been uploaded before posting
        time_to_sleep = resp_json['processing_info']['check_after_secs']
        status = False
        result = ''
        sleep(time_to_sleep)
        while not status:
            data = {
                'command' : 'STATUS',
                'media_id' : media_id
            }
            resp = ts.get(url=MEDIA_ENDPOINT_URL, params=data)
            resp_json = resp.json()['processing_info']
            result = resp_json['state']
            time_to_sleep = resp_json['check_after_secs']
            if not (result is 'failed' or result is 'succeeded'):
                sleep(time_to_sleep)
            else:
                status = True
                break

    if result is 'succeeded':
        #post tweet
        params = {
            'status' : '',
            'media_ids' : media_id
        }
        resp = ts.post(POST_TWEET_URL, data=params)
        if not resp.ok:
            print('Failed to upload tweet')
            print(resp.status_code)
            print(resp.text)

if __name__ == '__main__':
    main('','')