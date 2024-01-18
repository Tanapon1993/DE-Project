import boto3
from io import StringIO, BytesIO
import pandas as pd
from prefect import task, flow
import datetime
import psycopg2
import os
from sqlalchemy import create_engine

# # # fill your aws account name here
account_name = 'Tanapons' 

# read info from environment variables
# # before running this script, dont forget to assign environment variables using the `export` command in terminal
postgres_info = {
    'username': os.environ['postgres-username'],
    'password': os.environ['postgres-password'],
    'host': os.environ['postgres-host'],
    'port': os.environ['postgres-port'],
}

s3_client = boto3.client('s3')
bucket_name = 'fullstackdata2023'

def extract_folder(year, month, date):
   year_str = str(year)
   month_str = str(month).zfill(2)
   date_str = str(date).zfill(2)
   source_path = f'common/data/partitioned/{year_str}/{month_str}/{date_str}/transaction.csv'
   print(f"source_path is {source_path}")
   return source_path

@task(retries=3)
def extract(bucket_name, source_path):
    '''
    Extract data from S3 bucket
    '''
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
    # Read the CSV file content
    csv_string = StringIO(response['Body'].read().decode('utf-8'))
    # Use Pandas to read the CSV file
    df = pd.read_csv(csv_string)
    print(f"*** Extract df with {df.shape[0]} rows")
    return df
@task
def transform_s3(df):
    '''
    Transform data
    '''
    transaction = df.copy()
    transaction.rename(columns={'date': 'recency'}, inplace=True) 
    transaction_cols = ['customer_id', 'customer_name', 'customer_province', 'recency']
    customer = transaction[transaction_cols].drop_duplicates()
    customer['U_key'] = customer['customer_id'] + customer['customer_name'] + customer['customer_name']
    print(f"*** Transform df to customer with {customer.shape[1]} columns x {customer.shape[0]} rows")
    print(f"*** Columns are {customer.columns}")
    return customer

@task 
def load_postgres(customer):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] # each user has their own database in their username
    table_name = 'customer'
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    query = f"SELECT * FROM {table_name} ORDER BY customer_id"
    df = pd.read_sql(query, engine)
    print(df)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='append', index=False)
    print("Write successfully!")
 

# @task
# def load_parquet(customer):
#     '''
#     Load transformed result as parquet file to S3 bucket
#     '''
#     # Load data into S3 bucket
#     target_path = f'{account_name}/customer/customer.parquet'
#     parquet_buffer = BytesIO()
#     customer.to_parquet(parquet_buffer, index=False)
#     print(f"Uploading to bucket {bucket_name}, at path {target_path}")
#     s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=parquet_buffer.getvalue())
#     print("Upload successfully!")

@task
def extract_customer_postgres(table_name):
    '''
    Extract data from Postgres database
    '''
    database = postgres_info['username']  # each user has their own database in their username
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    query = f"SELECT * FROM {table_name} ORDER BY customer_id"
    df = pd.read_sql(query, engine)
    print(f"Read table: {table_name} from Postgres yielding {df.shape[0]} records")
    return df

@task
def transform(df):
    '''
    Transform data
    '''
    customer= df.loc[df.groupby('U_key')['recency'].idxmax()]
    return customer

@task
def load_postgres_back(customer):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] # each user has their own database in their username
    table_name = 'customer'
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Upserted {customer.shape[0]} records to {table_name} successfully!")
    print("Write back successfully!")

# @task 
# def load_csv(customer):
#     '''
#     Load transformed result as csv file to S3 bucket
#     '''
#     # Load data into S3 bucket
#     target_path = f'{account_name}/customer/customer.csv'
#     csv_buffer = StringIO()
#     customer.to_csv(csv_buffer, index=False)
#     print(f"Uploading to bucket {bucket_name}, at path {target_path}")
#     s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=csv_buffer.getvalue())
#     print("Upload successfully!")
   
@flow(log_prints=True)
def pipeline():
    '''
    Flow for ETL pipeline
    '''
    source_path = extract_folder(2023, 11, 30)
    df = extract(bucket_name, source_path)
    print("Extraction complete")
    customer = transform_s3(df)
    print("Transform complete")
    load_postgres(customer)
    # load_parquet(customer)
    print("Load complete")
    df = extract_customer_postgres(table_name='customer')
    customer = transform(df)
    load_postgres_back(customer)
    # print(customer)
    
    
if __name__ == '__main__':
    # pipeline()
    pipeline.serve(name="my_pipeline")
