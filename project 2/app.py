import pandas as pd
import sys
import glob
import os
import json
import re
from dotenv import load_dotenv

def get_column_names(schemas, ds_name, key='column_position'):
    schema = schemas.get(ds_name)
    columns = sorted(schema, key= lambda col: col[key])
    return [col['column_name'] for col in columns]

def read_csv(file, schema):
    file_path_list = re.split('[/\\\]', file)
    schema = file_path_list[-2]
    return pd.read_csv(file, names=columns, chunksize=10000)

def write_sql(schema, conncetion_url, df):
    for idx, chunk in enumerate(df):
        if idx == 0:
            chunk.to_sql(schema, conncetion_url, if_exists='replace', index=False)
        else:
            chunk.to_sql(schema, conncetion_url, if_exists='append', index=False)

if __name__ == '__main__':
    arg = None

    load_dotenv()
    src_base_dir = os.getenv('SRC_BASE_DIR')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    
    connection_url = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not arg:
        arg = schemas.keys()
    
    for key in arg:
        columns = get_column_names(schemas, key)
        file = glob.glob(f'{src_base_dir}/{key}/part-*')
      
        if(len(file[0]) == 0):
            raise NameError(f'No files found in {key}')
        
        df = read_csv(file[0], key)
        df = list(df)

        write_sql(key, connection_url, df)



        