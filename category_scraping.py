import json
import csv
import tempfile
import boto3
import time
import os
import requests

def api_request(country_code, api_key):
    request_url = f"https://youtube.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={country_code}&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        return None
    return request.json()
        

def write_csv(country_codes, api_key):
    all_categories = []    #init all categories list
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        # Column names
        fieldnames = ['id', 'title', 'assignable','country', 'category_date']
        csv_writer = csv.DictWriter(temp_file, quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
        csv_writer.writeheader()
        for country_code in country_codes:
            response = api_request(country_code, api_key)
            if response and 'items' in response:
                for item in response['items']:
                    item ['country_code'] = country_code
                    row = {
                        'id': item['id'],
                        'title': item['snippet']['title'],
                        'assignable': item['snippet']['assignable'],
                        'country': item['country_code'],
                        'category_date': time.strftime("%Y-%m-%d")
                        }
                    csv_writer.writerow(row)
    temp_file_path = temp_file.name
    return temp_file_path
        
def lambda_handler(event, context):
    # TODO implement
    api_key = os.getenv('api_key')
    bucket = os.getenv('bucket')
    
    with open('country_codes.txt') as file:
        country_codes = [x.strip() for x in file]

    temp_file_path = write_csv(country_codes, api_key)
    
    #Push to S3 
    s3_key = f'category/daily_category_scrap_{time.strftime("%Y.%m.%d")}.csv'
    #Upload in a S3 bucket
    with open(temp_file_path, 'rb') as file_obj:
        boto3.client('s3').upload_fileobj(file_obj, bucket, s3_key)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Push to s3 bucket  {bucket}  file  {s3_key} ')
    }
