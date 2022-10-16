import json
import psycopg2 as pg
from psycopg2 import errors
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine


schema_json = 'C:/Users/aldos/project_3_de/sql/schemas/user_address.json'
create_schema_sql = """create table user_address_2018_snapshots {};"""
zip_small_file = 'C:/Users/aldos/project_3_de/temp/dataset-small.zip'
small_file_name = 'dataset-small.csv'
database= 'shipping_orders'
user= 'postgres'
password= 'admin'
host= '127.0.0.1'
port='5432'
table_name = 'user_address_2018_snapshots'


with open (schema_json, 'r') as schema:
    content = json.loads(schema.read())
list_schema = []
for c in content:
    col_name =  c['column_name']
    col_type = c['column_type']
    constraint = c['is_null_able']
    ddl_list = [col_name, col_type, constraint]
    list_schema.append(ddl_list)        
    
list_schema2 = []
for s in list_schema:
    l = ' '.join(s)
    list_schema2.append(l)

create_schema_sql = """create table user_address_2018_snapshots {};"""
create_schema_sql_final = create_schema_sql.format(tuple(list_schema2)).replace("'", "")


#innit postgres conn
conn = pg.connect(database= database,
                  user= user,
                  password= password,
                  host= host,
                  port=port) 

conn.autocommit=True  
cursor=conn.cursor()
try:
    cursor.execute(create_schema_sql_final)
    print("ddl schema has sussecfully created...")
except pg.errors.DuplicateTable:
    print("table already exists...")

#load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)

col_name_df = [c['column_name'] for c in content]
df.columns = col_name_df


df_filtered = df[(df['created_at'] >= '2018-02-01') &(df['created_at'] < '2018-12-31') ]

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# insert to postgres

df_filtered.to_sql(table_name, engine, if_exists='append', index=False)
print(f'total inserted rows: {len(df_filtered)}')
print(f'initial created at: {df_filtered.created_at.min()}')
print(f'last created at: {df_filtered.created_at.max()}')





