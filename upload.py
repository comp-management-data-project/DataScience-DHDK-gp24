from impl import *

import os
from typing import Optional

# imports for relational db
from sqlite3 import connect
import json
from json import load

import sqlite3

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series




# Basic handlers 
# Hubert

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl


class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass



# Class to upload data from JSONs to SQLite database 
#Lucrezia

class ProcessDataUploadHandler(Handler):
    def __init__(self, db_name="relational_database.db"):
        super().__init__()
        self.db_name = db_name

    def process_data(self, json_data):
        object_id_mapping = self.map_object_ids(json_data)
        activity_dfs, tools_data = self.create_dataframes(json_data, object_id_mapping)
        return activity_dfs, tools_data

    def map_object_ids(self, json_data):
        object_id_mapping = {}
        for item in json_data:
            object_id = item['object id']
            object_internal_id = f"CH Object-{object_id}"
            object_id_mapping[object_id] = object_internal_id
        return object_id_mapping

    def create_dataframes(self, json_data, object_id_mapping):
        activity_dfs = {}
        tools_data = []
        for activity_type in json_data[0].keys():
            if activity_type != 'object id':
                activity_data = []
                activity_count = 1
                for item in json_data:
                    activity = item.get(activity_type, {})
                    if activity:
                        object_id = item['object id']
                        activity_internal_id = f"{activity_type.capitalize()}-{activity_count:02d}"
                        activity_count += 1
                        activity_data.append({
                            "Activity_internal_id": activity_internal_id,
                            "Refers To": object_id_mapping.get(object_id, ""),
                            "Responsible Institute": activity.get("responsible institute", ""),
                            "Responsible Person": activity.get("responsible person", ""),
                            "Technique": activity.get("technique", "") if activity_type == "acquisition" else "",
                            "Tool_internal_id": f"{activity_internal_id}-tool",
                            "Start Date": activity.get("start date", ""),
                            "End Date": activity.get("end date", ""),
                        })
                        if 'tool' in activity:
                            for tool in activity['tool']:
                                tools_data.append({
                                    "Tool_internal_id": f"{activity_internal_id}-tool",
                                    "Tool": tool,
                                    "Activity_internal_id": activity_internal_id
                                })
                activity_dfs[activity_type] = pd.DataFrame(activity_data)
        tools_df = pd.DataFrame(tools_data)
        return activity_dfs, tools_df

    def pushDataToDb(self, activity_dfs, tools_df):
        try:
            with connect(self.db_name) as conn:
                for activity_type, df in activity_dfs.items():
                    df.to_sql(activity_type.capitalize(), conn, if_exists='replace', index=False)
                tools_df.to_sql('Tools', conn, if_exists='replace', index=False)
        except Exception as e:
            print(f"Error occurred while pushing data to DB: {str(e)}")

    def handle_duplicates(self, df, conn, table_name):
        existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df = df[~df.duplicated(keep='first')]
        if not existing_data.empty:
            df = pd.concat([existing_data, df]).drop_duplicates(keep='first')
        return df


# Load JSON data
file_path = os.path.join("resources", "process.json")
with open(file_path, "r") as file:
    json_data = json.load(file)

# Instantiate the ProcessDataUploadHandler
process_data_upload_handler = ProcessDataUploadHandler()

# Process the data
activity_dfs, tools_df = process_data_upload_handler.process_data(json_data)

# Push the data to the database
process_data_upload_handler.pushDataToDb(activity_dfs, tools_df)




# Class to upload CSV files to Blazegraph
class MetadataUploadHandler(UploadHandler):
    def pushDataToDb(self, path):
        pass

