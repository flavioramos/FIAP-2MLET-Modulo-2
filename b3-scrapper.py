import datetime
import requests
import sys
import os
import boto3
import zipfile
import pandas as pd
from tqdm import tqdm

parquets_dir = ""
output_zip_file = ""
output_parquet_file = ""
date_string = ""
access_key_id = ""
secret_access_key = ""


def check_csv_file():
    global access_key_id
    global secret_access_key

    # check for file argument
    if len(sys.argv) != 2:
        print("Usage: python b3-scrapper.py <path_to_csv_file>")
        sys.exit(1)

    file_path = sys.argv[1]

    # check if file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)

    # check if csv is kinda valid
    with open(file_path, 'r') as file:
        lines = file.readlines()
        if len(lines) < 2:
            print("CSV file is not in the correct format or is missing data")
            sys.exit(1)

    # read credentials
    with open(file_path, 'r') as file:
        lines = file.readlines()
        access_key_id, secret_access_key = lines[1].strip().split(',')


def download_b3_zip():
    global output_zip_file
    global date_string

    # get last work day
    now = datetime.datetime.now()
    day = now.date() - datetime.timedelta(days=1)
    while day.weekday() >= 5:
        day -= datetime.timedelta(days=1)
    date_string = day.strftime('%Y-%m-%d')

    downloads_dir = f'{os.getcwd()}/downloads'
    output_zip_file = f'{downloads_dir}/{date_string}.zip'
    url = f'https://arquivos.b3.com.br/rapinegocios/tickercsv/{date_string}'

    # start download
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))

    # check for /downloads dir
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
    

    # write file
    with open(output_zip_file, 'wb') as file, tqdm(
        desc=f'Downloading {url}',
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)
            bar.update(len(data))

def generate_parquet():
    global output_parquet_file
    
    parquets_dir = f'{os.getcwd()}/raw'
    output_parquet_file = f'{parquets_dir}/{date_string}.parquet'

    # check for /parquets dir
    if not os.path.exists(parquets_dir):
            os.makedirs(parquets_dir)

    print(f'Reading .csv ...')
    # extract csv
    with zipfile.ZipFile(output_zip_file, 'r') as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            # read into pandas
            df = pd.read_csv(csv_file, sep=';')
            print(df)

    # save parquet
    print(f'Generating {date_string}.parquet ...')
    df.to_parquet(output_parquet_file, index=False)

def upload_to_s3():
    global access_key_id
    global secret_access_key
    global output_parquet_file

    bucket = "fiap-2024-flavio-mlet"
    prefix = "parquets"

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )
    s3_client = session.client('s3')

    print(f'Uploading {date_string}.parquet to s3://{bucket}/{prefix}/ ...')
    s3_client.upload_file(output_parquet_file, bucket, f'{prefix}/{date_string}.parquet')
    print(f'Upload complete, looking into bucket to confirm upload ...')

    paginator = s3_client.get_paginator('list_objects_v2')

    found = False
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'] == f'{prefix}/{date_string}.parquet':
                found = True

    if found:
        print("Upload successfull!")
    else:
        print("Could not find file on bucket!")
        sys.exit(1)


check_csv_file()
download_b3_zip()
generate_parquet()
upload_to_s3()