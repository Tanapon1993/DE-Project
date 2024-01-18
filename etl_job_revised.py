import boto3
from io import StringIO, BytesIO
import pandas as pd
from prefect import task, flow
import datetime
import psycopg2
import os
from sqlalchemy import create_engine

# # fill your aws account name here
account_name = 'Tanapons' 

# read info from environment variables
# before running this script, dont forget to assign environment variables using the `export` command in terminal
postgres_info = {
    'username': os.environ['postgres-username'],
    'password': os.environ['postgres-password'],
    'host': os.environ['postgres-host'],
    'port': os.environ['postgres-port'],
}

# Extract data from source
aws_access_key_id = 'AKIASZSZ6RCESSDHLTWN'
aws_secret_access_key = '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' 

session = boto3.Session(
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key
)

s3_client = session.client('s3')
bucket_name = 'fullstackdata2023'

start_date = datetime.datetime(2023, 11, 29)
end_date = datetime.datetime(2023, 12, 16)

# Create an empty list to store the datetimes
date_str = []

# Define a function to increment datetime and return a string
def increment_datetime(date):
    # Add one day to the date using timedelta
    date += datetime.timedelta(days=1)
    # Format the date as dd/mm/yyyy using strftime
    date_formatted = date.strftime("%Y/%m/%d")
    # Return the formatted date string
    return date_formatted

# Loop through dates starting from start_date until end_date
current_date = start_date
while current_date <= end_date:
    # Format the date and append to the list
    date_str.append(increment_datetime(current_date))
    # Increment the current date using the defined function
    current_date = current_date + datetime.timedelta(days=1)

# Create source paths for each date in the list
source_paths = [f'common/data/partitioned/{date}/transaction.csv' for date in date_str]

@task(retries=3)
def extract(bucket_name, source_paths):
    '''
    Extract data from S3 bucket
    '''
    dfs = [] 
    
    for source_path in source_paths:
        response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
        # Read the CSV file content
        csv_string = StringIO(response['Body'].read().decode('utf-8'))
        # Use Pandas to read the CSV file
        df = pd.read_csv(csv_string)
        dfs.append(df)     
    return dfs

@task
def transform(dfs):
    '''
    Transform data
    '''
    customer_dfs = [] 
    
    for df in dfs:
        transaction = df.copy()
        transaction.rename(columns={'date': 'recency'}, inplace=True) 
        grouped_transaction = transaction.groupby(['customer_id', 'customer_name', 'product_id', 'order_item_id', 'customer_province', 'product_category', 'product_name'], as_index=False)['recency'].max()
        customer_cols = ['customer_id', 'customer_name','product_category', 'product_name', 'customer_province', 'recency']
        customer = grouped_transaction[customer_cols].drop_duplicates()
    customer_dfs.append(customer)
    # Combine all individual customer DataFrames into a single DataFrame
    combined_customer = pd.concat(customer_dfs, ignore_index=True)
    print("Transform complete")
    return combined_customer

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
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Write successfully!")
 

# # @task
# # def load_parquet(customer):
# #     '''
# #     Load transformed result as parquet file to S3 bucket
# #     '''
# #     # Load data into S3 bucket
# #     target_path = f'{account_name}/customer/customer.parquet'
# #     parquet_buffer = BytesIO()
# #     customer.to_parquet(parquet_buffer, index=False)
# #     print(f"Uploading to bucket {bucket_name}, at path {target_path}")
# #     s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=parquet_buffer.getvalue())
# #     print("Upload successfully!")

# # @task
# def extract_customer_postgres(table_name):
#     '''
#     Extract data from Postgres database
#     '''
#     database = postgres_info['username']  # each user has their own database in their username
#     database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
#     engine = create_engine(database_url)
#     query = f"SELECT * FROM {table_name} ORDER BY customer_id"
#     df = pd.read_sql(query, engine)
#     print(f"Read table: {table_name} from Postgres yielding {df.shape[0]} records")
#     return df

# # @task
# def transform(df):
#     '''
#     Transform data
#     '''
#     customer= df.loc[df.groupby('U_key')['recency'].idxmax()]
#     return customer

# # @task
# def load_postgres_back(customer):
#     '''
#     Load transformed result to Postgres
#     '''
#     database = postgres_info['username'] # each user has their own database in their username
#     table_name = 'customer'
#     database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
#     engine = create_engine(database_url)
#     print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
#     customer.to_sql(table_name, engine, if_exists='replace', index=False)
#     print(f"Upserted {customer.shape[0]} records to {table_name} successfully!")
#     print("Write back successfully!")

# # @task 
# # def load_csv(customer):
# #     '''
# #     Load transformed result as csv file to S3 bucket
# #     '''
# #     # Load data into S3 bucket
# #     target_path = f'{account_name}/customer/customer.csv'
# #     csv_buffer = StringIO()
# #     customer.to_csv(csv_buffer, index=False)
# #     print(f"Uploading to bucket {bucket_name}, at path {target_path}")
# #     s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=csv_buffer.getvalue())
# #     print("Upload successfully!")
   
@flow(log_prints=True)
def pipeline():
    '''
    Flow for ETL pipeline
    '''
    dfs = extract(bucket_name, source_paths)
    customer = transform(dfs)
    load_postgres(customer)
    
if __name__ == '__main__':
    # pipeline()
    pipeline.serve(name="my_pipeline")