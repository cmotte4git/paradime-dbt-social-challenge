import json
import requests
import sys
import time
import os
import duckdb
import boto3

# Configuration de l'environnement
os.environ['HOME'] = '/tmp'
s3 = boto3.client('s3')

# Variables globales
snippet_features = ["title", "publishedAt", "channelId", "channelTitle", "categoryId"]
header = ["video_id"] + snippet_features + ["country", "trending_date", "tags", "view_count", "likes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]

unsafe_characters = ['\n', '\r', '"']

def prepare_feature(feature):
    """
    replace unsafe characters
    """
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def api_request(page_token, country_code, api_key):
    """
    Make an API call per country
    """
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()

def get_videos(items, country_code):
    """
    Extract data and output to csv format
    """
    lines = []
    for video in items:
        if "statistics" not in video:
            continue
        
        snippet = video['snippet']
        statistics = video['statistics']
        
        values = [
            video['id'],
            *[snippet.get(feature, "") for feature in snippet_features],
            country_code,
            time.strftime("%Y-%m-%d"),
            "|".join(snippet.get("tags", ["[none]"])),
            statistics.get("viewCount", 0),
            statistics.get('likeCount', 0),
            statistics.get('commentCount', 0),
            snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
            "commentCount" not in statistics,
            "likeCount" not in statistics,
            snippet.get("description", "")
        ]
        
        line = ",".join(prepare_feature(value) for value in values)
        lines.append(line)
    
    return lines

def get_pages(country_code, api_key, next_page_token="&"):
    
    country_data = []
    
    while next_page_token is not None:
        video_data_page = api_request(next_page_token, country_code, api_key)
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
        items = video_data_page.get('items', [])
        country_data += get_videos(items, country_code)
    
    return country_data

def get_data(conn, bucket, country_codes, api_key):
    """
    output to parquet and publish to S3 thanks to duckdb
    """
    conn.execute("CREATE TABLE all_countries (video_id VARCHAR, title VARCHAR, publishedAt VARCHAR, channelId VARCHAR, channelTitle VARCHAR, categoryId VARCHAR, country VARCHAR, trending_date VARCHAR, tags VARCHAR, view_count BIGINT, likes BIGINT, comment_count BIGINT, thumbnail_link VARCHAR, comments_disabled BOOLEAN, ratings_disabled BOOLEAN, description VARCHAR);")
    
    for country_code in country_codes:
        country_data = get_pages(country_code, api_key)
        temp_csv_path = f"/tmp/{country_code}_data.csv"
        with open(temp_csv_path, "w") as f:
            f.write("\n".join([",".join(header)] + country_data))
        conn.execute(f"INSERT INTO all_countries SELECT * FROM read_csv_auto('{temp_csv_path}')")
        os.remove(temp_csv_path)
    
    file = f's3://{bucket}/trending/daily_trending_scrap_{time.strftime("%Y.%m.%d")}.parquet'
    print(f"Saving to {file}")
    query = f"""
        COPY all_countries 
        TO '{file}' 
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    conn.execute(query)
    conn.close()

def lambda_handler(event, context):
    """
    main
    """
    try:
        api_key = os.getenv('api_key')
        bucket = os.getenv('bucket')
        aws_access_key_id = os.getenv('AK')
        aws_secret_access_key = os.getenv('secret')
        region = os.getenv('region')
        
        if not all([api_key, bucket, aws_access_key_id, aws_secret_access_key, region]):
            raise ValueError("Missing one or more necessary environment variables.")
        
        with open('country_codes.txt') as file:
            country_codes = [x.strip() for x in file]
        
        if not country_codes:
            raise ValueError("No country codes found.")
        
        print(f"Processing for countries: {country_codes}")
        
        conn = duckdb.connect(database=':memory:')
        conn.sql(f"""   
            CREATE SECRET secret1 (
                TYPE S3,
                KEY_ID '{aws_access_key_id}',
                SECRET '{aws_secret_access_key}',
                REGION '{region}'
            );
        """)
        
        get_data(conn, bucket, country_codes, api_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data processing and upload successful.')
        }
    
    except ValueError as ve:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Input error: {str(ve)}")
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"An error occurred: {str(e)}")
        }
