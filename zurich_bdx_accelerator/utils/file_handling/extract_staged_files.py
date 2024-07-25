"""Extract files from stage to table."""
import os

import pandas as pd
import snowflake.snowpark as snowpark


def extract_files_from_stage(session: snowpark.Session, stage):
    """List all files present in a SnowFlake stage.

        Args:
        session (snowpark.Session): Snowflake session
        stage (str) - Snowflake stage path to search

    Returns:
        file_list_df (pd.Dataframe): A dataframe containing a list of file paths
    """
    stage_dir = "@DEV.RAW.FILE_TYPE_1"
    file_list_rows = session.sql(
        "LIST {stage_dir}".format(stage_dir=stage_dir)
    ).collect()

    file_list = [row["name"] for row in file_list_rows]

    # Create a pandas DataFrame from the list of file paths
    # file_list_df = pd.DataFrame(file_paths, columns=['file_path'])

    return file_list


def file_to_table(session: snowpark.Session, file_list):
    """Convert a set of xlsx/csv files to individual tables in SnowFlake.

    If an xlsx has multiple sheets, each sheet will be converted into its own table.
        Args:
        session (snowpark.Session): Snowflake session
        file_list (list) - List of filepaths to convert

    Returns:
        return_msg (list): Message indication which files were converted
    """
    for filepath in file_list:
        # Extract the file from stage to /tmp dir for manipulation
        session.file.get(filepath, "/tmp")

        # Extract filename and extension
        filename = os.path.basename(filepath)
        f_name, f_ext = os.path.splitext(filename)
        full_path = f"/tmp/{filename}"

        df_dict = {}

        # Extract data to dict of dfs based on file extension.
        if f_ext == ".xlsx":
            try:
                df_dict = pd.read_excel(full_path, sheet_name=None, header=0)
                print("Successfully read {filename} into df".format(filename=filename))
            except Exception:
                print(pd.read_excel(full_path, sheet_name=None, header=0))
                print("Failed to convert {filename} into df".format(filename=filename))
                # exit

        elif f_ext == ".csv":
            try:
                # Writing to dict to match excel output.
                df_dict = {
                    "Sheet1": pd.read_csv(full_path, sep=",", engine="python", header=0)
                }
                print("Successfully read {filename} into df".format(filename=filename))
            except Exception:
                print(pd.read_csv(full_path, sep=",", engine="python", header=0))
                print("Failed to convert {filename} into df".format(filename=filename))
                # exit

        df_to_table(session, df_dict, filename, f_ext)

    return "File load complete."


def df_to_table(session: snowpark.Session, df_dict, filename, f_ext):
    """List all files present in a SnowFlake stage.

    Args:
    session (snowpark.Session): Snowflake session
    df_dict (dict): Dictionary of dataframes
    filenames (str): Filename that is being converted
    f_ext (str): Extension of file being converted
    """
    # CSV file or single shset workbook
    if len(df_dict) == 1:
        snf = session.createDataFrame(next(iter(df_dict.values())))

        table_name = "raw_temp_f2_" + f_ext[1:]

        snf.write.mode("overwrite").save_as_table(table_name)

        pre_rowcount = len(next(iter(df_dict.values())))
        print(
            """Succesfully loaded data for {filename} to {table_name}.
            {pre_rowcount} rows loaded.""".format(
                filename=filename, pre_rowcount=pre_rowcount, table_name=table_name
            )
        )
    # Multisheet workbook
    elif len(df_dict) > 1:
        for sheet in df_dict:
            snf = session.createDataFrame(df_dict[sheet])

            # Get row count from dataframe
            pre_rowcount = len(df_dict[sheet])
            table_name = "raw_temp_v2_f1_" + sheet + "_" + f_ext[1:]
            # Append records
            snf.write.mode("overwrite").save_as_table(table_name)

            print(
                """Succesfully loaded data for {filename} to {table_name}.
                 {pre_rowcount} rows loaded.""".format(
                    filename=filename, pre_rowcount=pre_rowcount, table_name=table_name
                )
            )

    # No data written to df dict
    else:
        print("No data for {filename} written to pandas df".format(filename=filename))
