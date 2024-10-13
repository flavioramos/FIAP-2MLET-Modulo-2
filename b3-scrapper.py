import datetime
import requests
import sys
import os
import boto3
import zipfile
import pandas as pd
import base64
import csv
import io
from tqdm import tqdm

parquets_dir = ""
output_parquet_file = ""
access_key_id = ""
secret_access_key = ""
csv_data = ""

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



def download_csv():
    global csv_data
    
    downloads_dir = f'{os.getcwd()}/downloads'
    output_csv_file = f'{downloads_dir}/bovespa.csv'
    url = f'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetDownloadPortfolioDay/eyJpbmRleCI6IklCT1YiLCJsYW5ndWFnZSI6InB0LWJyIn0='

    # start download
    response = requests.get(url)

    # check for /downloads dir
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
    
    base64_data = response.text

    csv_data = base64.b64decode(base64_data).decode('iso-8859-1')


def process_csv():
    global csv_data

    lines = csv_data.strip().split('\r\n')

    date = lines[0].split(' ')[-1]

    lines[1] = f'Data;{lines[1]}'

    lines = lines[1:-2]

    for i in range(1, len(lines)):
        lines[i] = f'{date};{lines[i]}'

    csv_data = '\n'.join(lines)


def generate_parquet():
    global csv_data
    global output_parquet_file
    
    parquets_dir = f'{os.getcwd()}/parquet'
    output_parquet_file = f'{parquets_dir}/bovespa.parquet'

    # check for /parquets dir
    if not os.path.exists(parquets_dir):
            os.makedirs(parquets_dir)

    print(f'Reading .csv ...')
    df = pd.read_csv(io.StringIO(csv_data), sep=';')
    print(df)

    # save parquet
    print(f'Generating bovespa.parquet ...')
    df.to_parquet(output_parquet_file, index=False)

def upload_to_s3():
    global access_key_id
    global secret_access_key
    global output_parquet_file

    bucket = "fiap-flavio-mlet-modulo-2"
    prefix = "raw"

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )
    s3_client = session.client('s3')

    print(f'Uploading bovespa.parquet to s3://{bucket}/{prefix}/ ...')
    s3_client.upload_file(output_parquet_file, bucket, f'{prefix}/bovespa.parquet')
    print(f'Upload complete, looking into bucket to confirm upload ...')

    paginator = s3_client.get_paginator('list_objects_v2')

    found = False
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'] == f'{prefix}/bovespa.parquet':
                found = True

    if found:
        print("Upload successfull!")
    else:
        print("Could not find file on bucket!")
        sys.exit(1)


check_csv_file()
download_csv()
process_csv()
generate_parquet()
upload_to_s3()