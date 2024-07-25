# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import os
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.session import Session
from dotenv import load_dotenv
import snowflake.snowpark.functions as F
from datetime import date
import time
import os
import pandas as pd
import openpyxl

load_dotenv(override=True)

def snowpark_session_create():
    USERNAME = os.getenv('USERNAME')
    PASSWORD = os.getenv('PASSWORD')
    connection_params = {
        "account": "fl77969.eu-west-2",
        "user": USERNAME,
        "password": PASSWORD,
        "role": "DEV_DATA_ENGINEER",
        "warehouse": "DEV_WAREHOUSE",
        "database": "DEV",
        "schema": "RAW"
    }
    session = Session.builder.configs(connection_params).create()
    return session

session = snowpark_session_create()

def file_to_table(session: snowpark.Session, filepath):   

    # Extract the file from stage to /tmp dir for manipulation
    staged_file = session.file.get(filepath, "/tmp")
    print(session.file.get(filepath, "/tmp"))
    
    # Extract filename and extension
    filename = os.path.basename(filepath)
    f_name, f_ext = os.path.splitext(filename)
    print(filename)
    full_path =  f"/tmp/{filename}"

    df_dict = {}

    if f_ext == '.xlsx':
        try:
            # sheet_name=None allows read_excel to extract all sheets into a dictionary of dfs (keys are sheet names)
            df_dict = pd.read_excel(full_path ,sheet_name=None, header=0)
            print("Successfully {filename} read into df".format(filename=filename))
            pd.read_excel(full_path ,sheet_name=None, header=0).head(10)
        except: 
            print( pd.read_excel(full_path , sheet_name=None, header=0))
            print("Failed to convert {filename} into df".format(filename=filename))
            #exit

    
    # elif f_ext =='.csv':
    #     try:
    #         # Writing to dict to match excel output. sep = None allows read_csv to autodetect the delimiter
    #         df_dict = {'Sheet1' : pd.read_csv(full_path , sep = None , header=0)}
    #         print("Successfully read {filename} into df".format(filename=filename))
    #     except: 
    #         print( pd.read_csv(full_path ,sep = ',', header=0))
    #         print("Failed to convert {filename} into df".format(filename=filename))
    #         #exit

    return_msg = []

    ### For single-sheet excel file
    if len(df_dict) == 1: 
        
        snf = session.createDataFrame(next(iter(df_dict.values())))

        snf.write.mode('overwrite').save_as_table("nn_raw_temp_f2")
        
        pre_rowcount = len(next(iter(df_dict.values())))
        return_msg.append(f"SUCCESS. Loaded {pre_rowcount} rows.")

    
    # Multisheet workbook
    # elif len(df_dict) > 1: 
    #     for sheet in df_dict:
    #         snf =  session.createDataFrame(df_dict[sheet])
        

    #         # Get row count from dataframe
    #         pre_rowcount = len(df_dict[sheet])

    #         # Append records
    #         snf.write.mode('overwrite').save_as_table("raw_temp_v2_f1_"+sheet+"_"+f_ext[1:])

    #         return_msg.append(f"SUCCESS. Loaded {pre_rowcount} rows.")

    # No data written to df dict
    else: 
        print("ERROR: No data written to pandas df")

    return return_msg

stage_dir = '@DEV.RAW.FILE_TYPE_2'
file_list_rows = session.sql('LIST {stage_dir}'.format(stage_dir=stage_dir)).collect()
print(file_list_rows)

file_paths = [row['name'] for row in file_list_rows]

# Create a pandas DataFrame from the list of file paths
file_list_df = pd.DataFrame(file_paths, columns=['file_path'])


#for file in  os.listdir(stage_dir): 
#    print(file)
#    file_to_table(session, file)
for index, row in file_list_df.iterrows():
    file_path = row['file_path']
    file_to_table(session, file_path)

print("Complete!")

